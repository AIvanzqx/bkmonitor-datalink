// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package etl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/transfer/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/transfer/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/transfer/json"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/transfer/logging"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/transfer/pipeline"
)

type LogClusterConfig struct {
	Address      string `mapstructure:"address" json:"address"`
	Timeout      string `mapstructure:"timeout" json:"timeout"`
	Retry        int    `mapstructure:"retry" json:"retry"`
	BatchSize    int    `mapstructure:"batch_size" json:"batch_size"`
	PollInterval string `mapstructure:"poll_interval" json:"poll_interval"`
}

func (c LogClusterConfig) GetTimeout() time.Duration {
	if c.Timeout == "" {
		return time.Minute
	}

	v, err := time.ParseDuration(c.Timeout)
	if err != nil || v <= 0 {
		return time.Minute
	}
	return v
}

func (c LogClusterConfig) GetPollInterval() time.Duration {
	if c.PollInterval == "" {
		return time.Second
	}

	v, err := time.ParseDuration(c.PollInterval)
	if err != nil || v <= 0 {
		return time.Second
	}
	return v
}

func (c LogClusterConfig) GetBatchSize() int {
	if c.BatchSize <= 0 {
		return 1000
	}
	return c.BatchSize
}

type LogCluster struct {
	*define.BaseDataProcessor
	*define.ProcessorMonitor

	conf  LogClusterConfig
	queue *innerQueue
	cli   *http.Client
	mut   sync.Mutex
}

type innerQueue struct {
	size int
	q    []*define.ETLRecord
}

func (iq *innerQueue) Push(record *define.ETLRecord) bool {
	iq.q = append(iq.q, record)
	return len(iq.q) >= iq.size
}

func (iq *innerQueue) Pop() []*define.ETLRecord {
	q := iq.q
	iq.q = make([]*define.ETLRecord, 0)
	return q
}

type LogClusterRequest struct {
	Index     string `json:"__index__"`
	Log       string `json:"log"`
	Timestamp int64  `json:"timestamp"`
}

type LogClusterResponse struct {
	Index        string `json:"__index__"`
	ID           string `json:"__id__"`
	GroupID      string `json:"__group_id__"`
	LogSignature string `json:"log_signature"`
	Pattern      string `json:"pattern"`
	IsNew        int    `json:"is_new"`
}

func (p *LogCluster) Process(d define.Payload, outputChan chan<- define.Payload, killChan chan<- error) {
	// 未初始话时透传即可
	if p.queue == nil {
		outputChan <- d
		return
	}

	p.mut.Lock() // 此 processor 会触发虚拟的 Process 事件 即有可能并发调用的情况 因此这里需要有个锁保护
	defer p.mut.Unlock()

	handle := func() error {
		batch := p.queue.Pop()
		if len(batch) == 0 {
			return nil
		}

		rsp, err := p.doRequest(batch)
		if err != nil {
			return err
		}

		for _, record := range rsp {
			output, err := define.DerivePayload(d, &record)
			if err != nil {
				logging.Errorf("%v create payload from %v error: %+v", p, d, err)
				continue
			}
			outputChan <- output
		}
		return nil
	}

	// 虚拟 Process 充当信号使用
	if d == nil {
		if err := handle(); err != nil {
			p.CounterFails.Inc()
		} else {
			p.CounterSuccesses.Inc()
		}
		return
	}

	var dst define.ETLRecord
	if err := d.To(&dst); err != nil {
		p.CounterFails.Inc()
		logging.Errorf("payload %v to record failed: %v", d, err)
		return
	}

	full := p.queue.Push(&dst)
	if !full {
		return
	}

	if err := handle(); err != nil {
		p.CounterFails.Inc()
	} else {
		p.CounterSuccesses.Inc()
	}
}

func (p *LogCluster) getLogField(metrics map[string]interface{}) string {
	if metrics == nil {
		return ""
	}

	log, ok := metrics["log"]
	if !ok {
		return ""
	}

	s, ok := log.(string)
	if !ok {
		return ""
	}
	return s
}

func (p *LogCluster) doRequest(records []*define.ETLRecord) ([]*define.ETLRecord, error) {
	items := make([]LogClusterRequest, 0, len(records))
	for i, record := range records {
		items = append(items, LogClusterRequest{
			Index:     strconv.Itoa(i),
			Log:       p.getLogField(record.Metrics),
			Timestamp: *record.Time,
		})
	}

	b, err := json.Marshal(map[string]interface{}{"data": items})
	if err != nil {
		return nil, err
	}

	rsp, err := p.cli.Post(p.conf.Address, "application/json", bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()

	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}

	type Response struct {
		Data []LogClusterResponse `json:"data"`
	}
	var r Response
	err = json.Unmarshal(body, &r)
	if err != nil {
		return nil, err
	}

	if len(r.Data) != len(records) {
		return nil, errors.New("records length not match")
	}

	for i := 0; i < len(records); i++ {
		records[i].Dimensions["log_signature"] = r.Data[i].LogSignature
		records[i].Dimensions["pattern"] = r.Data[i].Pattern
		records[i].Dimensions["is_new"] = strconv.Itoa(r.Data[i].IsNew)
	}
	return records, nil
}

func NewLogCluster(ctx context.Context, name string) (*LogCluster, error) {
	rtOption := config.PipelineConfigFromContext(ctx).Option
	unmarshal := func() (*LogClusterConfig, error) {
		v, ok := rtOption["log_cluster_config"]
		if !ok {
			return nil, nil
		}
		obj, ok := v.(map[string]interface{})
		if !ok {
			return nil, nil
		}

		var conf LogClusterConfig
		err := mapstructure.Decode(obj["log_cluster"], &conf)
		if err != nil {
			return nil, err
		}

		_, err = url.Parse(conf.Address)
		if err != nil {
			return nil, err
		}
		return &conf, nil
	}

	conf, err := unmarshal()
	if err != nil || conf == nil {
		logging.Errorf("failed to unmarshal log_cluster config: %v", err)
		return &LogCluster{
			BaseDataProcessor: define.NewBaseDataProcessor(name),
			ProcessorMonitor:  pipeline.NewDataProcessorMonitor(name, config.PipelineConfigFromContext(ctx)),
		}, nil
	}

	p := &LogCluster{
		BaseDataProcessor: define.NewBaseDataProcessor(name),
		ProcessorMonitor:  pipeline.NewDataProcessorMonitor(name, config.PipelineConfigFromContext(ctx)),
		conf:              *conf,
		queue: &innerQueue{
			size: conf.GetBatchSize(),
		},
		cli: &http.Client{
			Timeout: conf.GetTimeout(),
		},
	}
	p.SetPoll(conf.GetPollInterval())
	return p, nil
}

func init() {
	define.RegisterDataProcessor("log_cluster", func(ctx context.Context, name string) (processor define.DataProcessor, e error) {
		pipeConfig := config.PipelineConfigFromContext(ctx)
		if pipeConfig == nil {
			return nil, errors.Wrapf(define.ErrOperationForbidden, "pipeline config is empty")
		}
		rtConfig := config.ResultTableConfigFromContext(ctx)
		if pipeConfig == nil {
			return nil, errors.Wrapf(define.ErrOperationForbidden, "result table config is empty")
		}
		rtName := rtConfig.ResultTable
		name = fmt.Sprintf("%s:%s", name, rtName)
		return NewLogCluster(ctx, pipeConfig.FormatName(name))
	})
}

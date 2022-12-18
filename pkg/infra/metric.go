package infra

import "sync/atomic"

var (
	Metric = NewMetricInstance()
)

type MetricInstance struct {
	Abort int32
}

func NewMetricInstance() *MetricInstance {
	return &MetricInstance{
		Abort: 0,
	}
}

func (m *MetricInstance) AddAbort() {
	atomic.AddInt32(&m.Abort, 1)
}

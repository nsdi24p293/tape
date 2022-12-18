package infra

import (
	"fmt"
	"sort"
	"time"

	"github.com/osdi23p228/fabric-protos-go/peer"
)

var (
	timeKeepers TimeKeepers
)

type TimeKeepers struct {
	transactions        []*TimeKeeper
	endorseLatency      []int64
	orderCommitLatency  []int64
	totalLatency        []int64
	commitLatencySorted []int64
}

type TimeKeeper struct {
	ProposedTime  int64
	EndorsedTime  int64
	BroadcastTime int64
	ObservedTime  int64
}

func initTimeKeepers() {
	timeKeepers = TimeKeepers{
		transactions:        make([]*TimeKeeper, config.TxNum),
		endorseLatency:      make([]int64, config.TxNum),
		orderCommitLatency:  make([]int64, config.TxNum),
		totalLatency:        make([]int64, config.TxNum),
		commitLatencySorted: nil,
	}
	for i := range timeKeepers.transactions {
		timeKeepers.transactions[i] = &TimeKeeper{}
	}
}

func (tks *TimeKeepers) keepProposedTime(
	txid string,
	endorserIndex int,
	connIndex int,
	clientIndex int,
) {
	proposedTime := time.Now().UnixNano()

	id := txid2id[txid]
	logCh <- fmt.Sprintf("%-10s %d %4d %s %d %d %d", "Proposed", proposedTime, id, txid, endorserIndex, connIndex, clientIndex)

	timeKeepers.transactions[id].ProposedTime = proposedTime
}

func (tks *TimeKeepers) keepEndorsedTime(
	txid string,
	endorserIndex int,
	connIndex int,
	clientIndex int,
) {
	endorsedTime := time.Now().UnixNano()

	id := txid2id[txid]
	logCh <- fmt.Sprintf("%-10s %d %4d %s %d %d %d", "Endorsed", endorsedTime, id, txid, endorserIndex, connIndex, clientIndex)

	timeKeepers.transactions[id].EndorsedTime = endorsedTime
	timeKeepers.endorseLatency[id] = endorsedTime - timeKeepers.transactions[id].ProposedTime
}

func (tks *TimeKeepers) keepBroadcastTime(
	txid string,
	broadcasterIndex int,
) {
	broadcastTime := time.Now().UnixNano()

	id := txid2id[txid]
	logCh <- fmt.Sprintf("%-10s %d %4d %s %d", "Broadcast", broadcastTime, id, txid, broadcasterIndex)

	timeKeepers.transactions[id].BroadcastTime = broadcastTime
}

func (tks *TimeKeepers) keepObservedTime(
	txid string,
	validationCode peer.TxValidationCode,
) {
	observedTime := time.Now().UnixNano()

	id := txid2id[txid]
	logCh <- fmt.Sprintf("%-10s %d %4d %s %s", "Observed", observedTime, id, txid, validationCode)

	timeKeepers.transactions[id].ObservedTime = observedTime
	timeKeepers.totalLatency[id] = observedTime - timeKeepers.transactions[id].ProposedTime
	timeKeepers.orderCommitLatency[id] = observedTime - timeKeepers.transactions[id].BroadcastTime
}

func (tks *TimeKeepers) getAverageTotalLatency() float64 {
	// var result int64 = 0
	// for _, cl := range tks.totalLatency {
	// 	result += cl
	// }
	// return float64(result) / float64(config.TxNum) / 1e9
	return tks.getAverageLatencyFromSlice(&tks.totalLatency)
}

func (tks *TimeKeepers) getAverageEndorseLatency() float64 {
	// var result int64 = 0
	// for _, cl := range tks.endorseLatency {
	// 	result += cl
	// }
	// return float64(result) / float64(config.TxNum) / 1e9
	return tks.getAverageLatencyFromSlice(&tks.endorseLatency)
}

func (tks *TimeKeepers) getAverageOrderCommitLatency() float64 {
	// var result int64 = 0
	// for _, cl := range tks.orderCommitLatency {
	// 	result += cl
	// }
	// return float64(result) / float64(config.TxNum) / 1e9
	return tks.getAverageLatencyFromSlice(&tks.orderCommitLatency)
}

func (tks *TimeKeepers) getAverageLatencyFromSlice(slice *[]int64) float64 {
	var result int64 = 0
	for _, cl := range *slice {
		result += cl
	}
	return float64(result) / float64(config.TxNum) / 1e9
}

func (tks *TimeKeepers) getCommitLatencyOfPercentile(p int) float64 {
	if tks.commitLatencySorted == nil {
		tks.sortCommitLatency()
	}

	index := int(float64(p) / 100.0 * float64(config.TxNum))
	if index < 0 {
		index = 0
	} else if index >= config.TxNum {
		index = config.TxNum - 1
	}

	return float64(tks.commitLatencySorted[index]) / 1e9
}

func (tks *TimeKeepers) sortCommitLatency() {
	tks.commitLatencySorted = make([]int64, len(tks.totalLatency))
	copy(tks.commitLatencySorted, tks.totalLatency)
	sort.Slice(
		tks.commitLatencySorted,
		func(i, j int) bool {
			return tks.commitLatencySorted[i] < tks.commitLatencySorted[j]
		},
	)
}

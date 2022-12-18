package infra

import (
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	CH_MAX_CAPACITY     = 1e6
	endorsementFilename = "ENDORSEMENT.txt"
)

var (
	txid2id map[string]int
	config  *Config
	logger  *log.Logger
)

var (
	logCh         chan string
	reportCh      chan string
	unsignedCh    chan *Element
	signedChs     []chan *Element
	endorsedCh    chan *Element
	integratedCh  chan *Element
	observerEndCh chan struct{}
	doneCh        chan struct{}
)

// isBreakdownPhase1 returns true if this round is phase 1,
// false if this round is phase 2
func isBreakdownPhase1() bool {
	_, err := os.Stat(endorsementFilename)
	return err != nil
}

func Process(c *Config, l *log.Logger) {
	txid2id = make(map[string]int)
	config = c
	logger = l

	if config.End2End {
		logger.Info("Test Mode: End To End")
		End2End()
	} else {
		panic("Tape does NOT support breakdown mode")
	}
}

// WriteLogToFile receives and write the following types of log to file:
//
//	Start: timestamp txid-index txid  endorser-id, connection-id, client-id
//	Proposal: timestamp txid-index txid  endorser-id, connection-id, client-id
//	Broadcast: timestamp txid-index txid  broadcaster-id
//	End: timestamp txid-index txid [VALID/MVCC]
//	Number of all transactions: total-transaction-num
//	Number of VALID transactions: valid-transaction-num
//	Number of ABORTED transactions: aborted-transaction-num
//	Abort rate: abort-rate
//	Duration: duration
//	TPS: throughput
func WriteLogToFile(printWG *sync.WaitGroup) {
	printWG.Add(1)
	defer printWG.Done()

	logFile, err := os.Create(config.LogPath)
	if err != nil {
		logger.Fatalf("Failed to create log file %s: %v\n", config.LogPath, err)
	}
	defer logFile.Close()

	reportFile, err := os.Create(config.ReportPath)
	if err != nil {
		logger.Fatalf("Failed to create report file %s: %v\n", config.ReportPath, err)
	}
	defer reportFile.Close()

	for {
		select {
		case s := <-logCh:
			logFile.WriteString(s + "\n")
		case s := <-reportCh:
			reportFile.WriteString(s + "\n")
		case <-doneCh:
			for len(logCh) > 0 {
				logFile.WriteString(<-logCh + "\n")
			}
			for len(reportCh) > 0 {
				reportFile.WriteString(<-reportCh + "\n")
			}
			return
		}
	}
}

func NewLogChannel() chan string {
	logCh := make(chan string, CH_MAX_CAPACITY)
	return logCh
}

func NewReportChannel() chan string {
	reportCh := make(chan string, CH_MAX_CAPACITY)
	return reportCh
}

func NewUnsignedChannel() chan *Element {
	// unsignedCh stores all unsigned transactions
	// Sender: initiator
	// Receiver: signers
	unsignedCh := make(chan *Element, CH_MAX_CAPACITY)
	return unsignedCh
}

func NewSignedChannel() []chan *Element {
	// signedChs are a set of channels, each of which is for one endorser
	// and stores all signed but not yet endorsed transactions
	// Sender: signers
	// Receiver: proposers
	signedChs := make([]chan *Element, config.EndorserNum)
	for i := 0; i < config.EndorserNum; i++ {
		signedChs[i] = make(chan *Element, CH_MAX_CAPACITY)
	}
	return signedChs
}

func NewEndorsedChannel() chan *Element {
	// endorsedCh stores all endorsed but not yet extracted transactions
	// Sender: proposers
	// Receiver: integrators
	endorsedCh := make(chan *Element, config.Burst)
	return endorsedCh
}

func NewIntegratedChannel() chan *Element {
	// integratedCh stores all endorsed envelope-format transactions
	// Sender: integrators
	// Receiver: broadcasters
	integratedCh := make(chan *Element, config.Burst)
	return integratedCh
}

func NewObserverEndChannel() chan struct{} {
	observerEndCh := make(chan struct{})
	return observerEndCh
}

func initDoneChannel() chan struct{} {
	doneCh := make(chan struct{})
	return doneCh
}

func initChannels() {
	logCh = NewLogChannel()
	reportCh = NewReportChannel()
	unsignedCh = NewUnsignedChannel()
	signedChs = NewSignedChannel()
	endorsedCh = NewEndorsedChannel()
	integratedCh = NewIntegratedChannel()
	observerEndCh = NewObserverEndChannel()
	doneCh = initDoneChannel()
}

func WaitObserverEnd(startTime time.Time, printWG *sync.WaitGroup) {
	select {
	case <-observerEndCh:
		duration := time.Since(startTime)
		logger.Infof("Finish processing transactions")

		reportCh <- fmt.Sprintf("ALL Transactions: %d", config.TxNum)
		reportCh <- fmt.Sprintf("VALID Transactions: %d", int32(config.TxNum)-Metric.Abort)
		reportCh <- fmt.Sprintf("ABORTED Transactions: %d", Metric.Abort)
		reportCh <- fmt.Sprintf("Duration: %.3fs", float64(duration.Milliseconds())/float64(1e3))
		reportCh <- fmt.Sprintf("TPS: %.3f", float64(config.TxNum)*1e9/float64(duration.Nanoseconds()))
		reportCh <- fmt.Sprintf("Effective TPS: %.3f", float64(int32(config.TxNum)-Metric.Abort)*1e9/float64(duration.Nanoseconds()))
		reportCh <- fmt.Sprintf("Abort Rate: %.3f%%", float64(Metric.Abort)/float64(config.TxNum)*100)
		reportCh <- fmt.Sprintf("Average Commit Latency: %.3fs", timeKeepers.getAverageTotalLatency())
		reportCh <- fmt.Sprintf("Average Endorse Latency: %.3fs", timeKeepers.getAverageEndorseLatency())
		reportCh <- fmt.Sprintf("Average Order&Commit Latency: %.3fs", timeKeepers.getAverageOrderCommitLatency())

		percentiles := []int{50, 55, 60, 65, 70, 75, 80, 85, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100}
		for _, i := range percentiles {
			reportCh <- fmt.Sprintf("Commit Latency [%d%%]: %.3fs", i, timeKeepers.getCommitLatencyOfPercentile(i))
		}

		reportCh <- fmt.Sprintf("id    endorse(ms) integrate(ms) order&commit(ms)")
		for i, tk := range timeKeepers.transactions {
			endorsementDuration := float64(tk.EndorsedTime-tk.ProposedTime) / float64(1e6)
			if endorsementDuration < 0.0 {
				endorsementDuration = 0.0
			}

			integrationDuration := float64(tk.BroadcastTime-tk.EndorsedTime) / float64(1e6)
			if integrationDuration < 0.0 {
				integrationDuration = 0.0
			}

			orderingDuration := float64(tk.ObservedTime-tk.BroadcastTime) / float64(1e6)
			if orderingDuration < 0.0 {
				orderingDuration = 0.0
			}

			reportCh <- fmt.Sprintf("%-5d %11.2f %13.2f %16.2f",
				config.TxIDStart+i,
				endorsementDuration,
				integrationDuration,
				orderingDuration,
			)
		}

		// Closing 'doneCh', a channel which is never sent an element, is a common technique to notify ending in Golang
		// More information: https://go101.org/article/channel-use-cases.html#check-closed-status
		close(doneCh)

		// Wait for WriteLogToFile() to return
		printWG.Wait()
	}
}

// End2End executes end-to-end benchmark on HLF
// An Element (i.e. a transaction) will go through the following channels
// unsignedCh -> signedCh -> endorsedCh -> integratedCh
func End2End() {
	initChannels()
	initTimeKeepers()

	printWG := &sync.WaitGroup{}
	go WriteLogToFile(printWG)

	initiator := NewInitiator(unsignedCh)
	signer := NewSigner(unsignedCh, signedChs)
	proposers := NewProposers(signedChs, endorsedCh)
	integrators := NewIntegrators(endorsedCh, integratedCh)
	broadcasters := NewBroadcasters(integratedCh)
	observer := NewObserver()

	integrators.StartAsync()
	broadcasters.StartAsync()
	observer.StartAsync()
	initiator.StartSync() // Block until all raw transactions are ready
	signer.StartSync()    // Block until all transactions are signed

	startTime := time.Now()
	proposers.StartAsync()

	WaitObserverEnd(startTime, printWG)
}

package infra

import (
	"time"

	"github.com/osdi23p228/fabric-protos-go/peer"
)

type Observer struct {
	client    peer.Deliver_DeliverFilteredClient
	deliverCh chan *peer.DeliverResponse_FilteredBlock
}

func NewObserver() *Observer {
	deliverer, err := CreateDeliverFilteredClient()
	if err != nil {
		logger.Fatalf("Fail to create DeliverFilteredClient: %v", err)
	}

	envelope, err := CreateSignedDeliverNewestEnv()
	if err != nil {
		logger.Fatalf("Fail to create SignedEnvelope: %v", err)
	}

	if err = deliverer.Send(envelope); err != nil {
		logger.Fatalf("Fail to send SignedEnvelope: %v", err)
	}

	// drain the first response
	if _, err = deliverer.Recv(); err != nil {
		logger.Fatalf("Fail to receive the first response: %v", err)
	}

	return &Observer{
		client:    deliverer,
		deliverCh: make(chan *peer.DeliverResponse_FilteredBlock),
	}
}

// StartAsync starts observing
func (o *Observer) StartAsync() {
	logger.Infof("Start observer")

	// Process FilteredBlock
	go o.processFilteredBlock()

	go o.receiveFilteredBlock()
}

func (o *Observer) processFilteredBlock() {
	var validTxNum int32 = 0
	for {
		select {
		case fb := <-o.deliverCh:
			for _, tx := range fb.FilteredBlock.FilteredTransactions {
				timeKeepers.keepObservedTime(tx.GetTxid(), tx.TxValidationCode)
			}

			for _, tx := range fb.FilteredBlock.FilteredTransactions {
				if tx.TxValidationCode == peer.TxValidationCode_VALID {
					validTxNum += 1
				} else {
					Metric.AddAbort()
				}
			}

			if validTxNum+Metric.Abort >= int32(config.TxNum) {
				close(observerEndCh)
				return
			}
		case <-time.After(30 * time.Second):
			close(observerEndCh)
			return
		case <-doneCh:
			o.client.CloseSend()
			return
		}
	}
}

func (o *Observer) receiveFilteredBlock() {
	for {
		deliverResponse, err := o.client.Recv()
		if err != nil {
			logger.Fatalln("Fail to receive deliver response: %v", err)
		}
		if deliverResponse == nil {
			logger.Fatalln("Received a nil DeliverResponse")
		}

		switch t := deliverResponse.Type.(type) {
		case *peer.DeliverResponse_FilteredBlock:
			o.deliverCh <- t
		case *peer.DeliverResponse_Status:
			logger.Infoln("Status:", t.Status)
		default:
			logger.Infoln("Unknown DeliverResponse type")
		}
	}
}

package infra

import (
	"io"
	"time"

	"github.com/osdi23p228/fabric-protos-go/common"
	"github.com/osdi23p228/fabric-protos-go/orderer"
)

type Broadcasters struct {
	broadcasters []*Broadcaster
	tokenCh      chan struct{}
}

func NewBroadcasters(inCh <-chan *Element) *Broadcasters {
	bs := &Broadcasters{
		broadcasters: make([]*Broadcaster, config.BroadcasterNum),
		tokenCh:      make(chan struct{}, int(config.Burst)),
	}

	// The expect throughput for each broadcaster
	expectTPS := float64(config.Rate) / float64(config.BroadcasterNum)

	for i := 0; i < config.BroadcasterNum; i++ {
		client, err := CreateBroadcastClient(config.Orderer)
		if err != nil {
			logger.Fatalf("Fail to create connection for the No. %d broadcaster: %v", i, err)
		}

		bs.broadcasters[i] = &Broadcaster{
			client:           client,
			broadcasterIndex: i,
			expectTPS:        expectTPS,
			inCh:             inCh,
			tokenCh:          bs.tokenCh,
		}
	}

	return bs
}

// StartAsync starts a goroutine for every broadcaster
func (bs *Broadcasters) StartAsync() {
	// Use a token bucket to throttle the sending of envelopes
	go bs.generateTokens()

	// Start multiple goroutines to send envelopes
	for _, b := range bs.broadcasters {
		go b.receive()
		go b.send()
	}
}

func (bs *Broadcasters) generateTokens() {
	if config.Rate == 0 {
		for {
			bs.tokenCh <- struct{}{}
		}
	} else {
		interval := 1e9 / config.Rate
		for {
			bs.tokenCh <- struct{}{}
			time.Sleep(time.Duration(interval) * time.Nanosecond)
		}
	}
}

type Broadcaster struct {
	client           orderer.AtomicBroadcast_BroadcastClient
	broadcasterIndex int
	expectTPS        float64
	inCh             <-chan *Element
	tokenCh          chan struct{}
}

func (b *Broadcaster) getToken() {
	<-b.tokenCh
}

// send collects and send envelopes to the orderer
func (b *Broadcaster) send() {
	logger.Infof("Start broadcasting")

	for {
		select {
		case element := <-b.inCh:
			b.getToken()

			timeKeepers.keepBroadcastTime(element.Txid, b.broadcasterIndex)

			err := b.client.Send(element.Envelope)
			if err != nil {
				logger.Fatalln(err)
			}
		case <-doneCh:
			b.client.CloseSend()
			return
		}
	}
}

func (b *Broadcaster) receive() {
	for {
		res, err := b.client.Recv()
		if err != nil {
			if err != io.EOF {
				logger.Errorf("Recieve broadcast error: %+v, status: %+v\n", err, res)
			}
			return
		}

		if res.Status != common.Status_SUCCESS {
			logger.Fatalf("Receive error status %s", res.Status)
		}
	}
}

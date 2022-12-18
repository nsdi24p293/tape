package infra

import (
	"context"
	"time"

	"github.com/osdi23p228/fabric-protos-go/peer"
)

type Proposers struct {
	proposers [][][]*Proposer
	tokenCh   chan struct{}
	inChs     []chan *Element
}

func NewProposers(inChs []chan *Element, outCh chan *Element) *Proposers {
	proposers := make([][][]*Proposer, config.EndorserNum)
	tokenCh := make(chan struct{}, int(config.Burst))
	expectTPS := float64(config.Rate) / float64(config.ConnNum*config.ClientPerConnNum)

	for i, endorser := range config.Endorsers {
		proposers[i] = make([][]*Proposer, config.ConnNum)

		for j := 0; j < config.ConnNum; j++ {
			proposers[i][j] = make([]*Proposer, config.ClientPerConnNum)

			grpcClient, err := CreateEndorserClient(endorser)
			if err != nil {
				logger.Fatalf("Fail to create No. %d connection for endorser %s: %v", j, endorser.Address, err)
			}

			for k := 0; k < config.ClientPerConnNum; k++ {
				proposers[i][j][k] = &Proposer{
					endorserIndex: i,
					connIndex:     j,
					clientIndex:   k,
					expectTPS:     expectTPS,
					grpcClient:    grpcClient,
					address:       endorser.Address,
					inCh:          make(chan *Element, CH_MAX_CAPACITY),
					outCh:         outCh,
					tokenCh:       tokenCh,
				}
			}
		}
	}

	return &Proposers{
		proposers: proposers,
		tokenCh:   tokenCh,
		inChs:     inChs,
	}
}

// StartAsync starts a goroutine as proposer per client per connection per endorser
func (ps *Proposers) StartAsync() {
	logger.Infof("Start sending transactions")

	// Use a token bucket to throttle the sending of proposals
	go ps.generateTokens()

	for i, ch := range ps.inChs {
		go ps.dispatchElements(i, ch)
	}

	for i := 0; i < config.EndorserNum; i++ {
		for j := 0; j < config.ConnNum; j++ {
			for k := 0; k < config.ClientPerConnNum; k++ {
				go ps.proposers[i][j][k].Start()
			}
		}
	}
}

func (ps *Proposers) generateTokens() {
	if config.Rate == 0 {
		for {
			ps.tokenCh <- struct{}{}
		}
	} else {
		interval := 1e9 / float64(config.Rate) * float64(config.EndorserNum)
		for {
			time.Sleep(time.Duration(interval) * time.Nanosecond)
			ps.tokenCh <- struct{}{}
		}
	}
}

func (ps *Proposers) dispatchElements(endorserIndex int, ch chan *Element) {
	for {
		e := <-ch
		connIndex, clientIndex := parseElementIndexes(e)
		ps.proposers[endorserIndex][connIndex][clientIndex].inCh <- e
	}
}

func parseElementIndexes(e *Element) (int, int) {
	sequence := txid2id[e.Txid]
	connIndex := (sequence / config.ClientPerConnNum) % config.ConnNum
	clientIndex := sequence % config.ClientPerConnNum
	return connIndex, clientIndex
}

type Proposer struct {
	endorserIndex int
	connIndex     int
	clientIndex   int
	expectTPS     float64
	grpcClient    peer.EndorserClient
	address       string
	inCh          chan *Element
	outCh         chan *Element
	tokenCh       chan struct{}
}

func (p *Proposer) getToken() {
	<-p.tokenCh
}

type ProposerClient struct {
	clientIndex int
}

// Start serves as the k-th client of the j-th connection to the endorser specified by channel 'signed'.
// It collects signed proposals and send them to the endorser
func (p *Proposer) Start() {
	for {
		select {
		case element := <-p.inCh:
			// Send signed proposal to peer for endorsement

			p.getToken()

			timeKeepers.keepProposedTime(element.Txid, p.endorserIndex, p.connIndex, p.clientIndex)

			// send proposal
			resp, err := p.grpcClient.ProcessProposal(context.Background(), element.SignedProposal)
			if err != nil || resp.Response.Status < 200 || resp.Response.Status >= 400 {
				if resp == nil {
					logger.Errorf("Error processing proposal: %v, status: unknown, address: %s \n", err, p.address)
				} else {
					logger.Errorf("Error processing proposal: %v, status: %d, message: %s, address: %s \n", err, resp.Response.Status, resp.Response.Message, p.address)
				}
				continue
			}

			element.lock.Lock()
			element.Responses = append(element.Responses, resp)
			if len(element.Responses) >= config.EndorserNum {
				// Collect enough endorsement for this transaction
				p.outCh <- element

				timeKeepers.keepEndorsedTime(element.Txid, p.endorserIndex, p.connIndex, p.clientIndex)
			}
			element.lock.Unlock()

		case <-doneCh:
			return
		}
	}
}

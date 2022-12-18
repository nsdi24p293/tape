package infra

import (
	"strconv"

	"github.com/osdi23p228/fabric-protos-go/peer"
)

type Initiator struct {
	proposals []*peer.Proposal
	txids     []string
	outCh     chan *Element
}

func NewInitiator(outCh chan *Element) *Initiator {
	it := &Initiator{
		proposals: make([]*peer.Proposal, config.TxNum),
		txids:     make([]string, config.TxNum),
		outCh:     outCh,
	}

	// Create proposal and id for all generated transactions
	ccArgsList := generateCCArgsList()
	session := getSession()
	for i := 0; i < config.TxNum; i++ {
		ccArgs := ccArgsList[i]

		tempTXID := ""
		if !config.CheckTxID {
			tempTXID = generateCustomTXID(i, session)
		}

		proposal, txID, err := CreateProposal(
			tempTXID,
			config.Channel,
			config.Chaincode,
			config.Version,
			ccArgs,
		)
		if err != nil {
			logger.Fatalf("Fail to create proposal %s: %v", txID, err)
		}

		txid2id[txID] = i
		it.proposals[i] = proposal
		it.txids[i] = txID
	}

	return it
}

func getSession() string {
	if config.Session != "" {
		return config.Session
	}
	return getName(20)
}

func generateCustomTXID(i int, session string) string {
	return strconv.Itoa(config.TxIDStart+i) + "_+=+_" + session + "_+=+_" + getName(20)
}

// StartSync sends all unsigned transactions (raw transactions) to the channel 'raw'
// waiting for subsequent processing
func (it *Initiator) StartSync() {
	for i := 0; i < len(it.proposals); i++ {
		it.outCh <- &Element{Proposal: it.proposals[i], Txid: it.txids[i]}
	}

	it.End()
}

func (it *Initiator) End() {
	it.outCh <- nil
}

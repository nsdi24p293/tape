package infra

type Signers struct {
	Signers []*Signer
}

type Signer struct {
	inCh  chan *Element
	outCh []chan *Element
}

func NewSigner(inCh chan *Element, outCh []chan *Element) *Signer {
	return &Signer{
		inCh:  inCh,
		outCh: outCh,
	}
}

// StartSync collects an unsigned transactions from the 'raw' channel,
// sign it, then send it to the 'signed' channel of each endorser
func (s *Signer) StartSync() {
	for {
		select {
		case e := <-s.inCh:
			if e == nil { // End
				return
			}

			// sign the raw transaction
			err := s.SignElement(e)
			if err != nil {
				logger.Fatalf("Fail to sign transaction %s: %v", e.Txid, err)
			}

			// send the signed transactions to each endorser's proposers
			startIndex := 0
			endIndex := config.EndorserNum
			for i := startIndex; i < endIndex; i++ {
				s.outCh[i] <- e
			}

		case <-doneCh:
			return
		}
	}
}

// SignElement signs a transaction with the assembler's identity
func (s *Signer) SignElement(e *Element) error {
	signedProposal, err := SignProposal(e.Proposal)
	if err != nil {
		return err
	}
	e.SignedProposal = signedProposal

	return nil
}

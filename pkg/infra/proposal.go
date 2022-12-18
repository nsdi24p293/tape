package infra

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math"

	"github.com/golang/protobuf/proto"
	"github.com/osdi23p228/fabric-protos-go/common"
	"github.com/osdi23p228/fabric-protos-go/orderer"
	"github.com/osdi23p228/fabric-protos-go/peer"
	"github.com/osdi23p228/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/osdi23p228/fabric/protoutil"
	"github.com/pkg/errors"
)

func getRandomNonce() ([]byte, error) {
	key := make([]byte, 24)

	_, err := rand.Read(key)
	if err != nil {
		return nil, errors.Wrap(err, "error getting random bytes")
	}
	return key, nil
}

// CreateProposal creates an unsigned proposal based on the given information and returns a proposal and its transaction id
func CreateProposal(txid string, channel, ccname, version string, args []string) (*peer.Proposal, string, error) {
	// convert the argument list to a byte list
	var argsByte [][]byte
	for _, arg := range args {
		argsByte = append(argsByte, []byte(arg))
	}

	// create an chaincode invocation object
	spec := &peer.ChaincodeSpec{
		Type:        peer.ChaincodeSpec_GOLANG,
		ChaincodeId: &peer.ChaincodeID{Name: ccname, Version: version},
		Input:       &peer.ChaincodeInput{Args: argsByte},
	}
	invocation := &peer.ChaincodeInvocationSpec{ChaincodeSpec: spec}

	// use the client's identity provided in the configuration file
	creator, err := config.Identity.Serialize()
	if err != nil {
		return nil, "", err
	}

	if txid == "" {
		// if transaction id is not provided, let the protoutil decides the ID
		prop, txid, err := protoutil.CreateChaincodeProposal(common.HeaderType_ENDORSER_TRANSACTION, channel, invocation, creator)
		if err != nil {
			return nil, "", err
		}
		return prop, txid, nil
	} else {
		// To use a customized ID, we MUST disable txid check in
		// core/endorser/msgvalidation.go:Validate and protoutil/proputils.go:ComputeTxID (v2)
		nonce, err := getRandomNonce()
		prop, txid, err := protoutil.CreateChaincodeProposalWithTxIDNonceAndTransient(txid, common.HeaderType_ENDORSER_TRANSACTION, channel, invocation, nonce, creator, nil)
		if err != nil {
			return nil, "", err
		}
		return prop, txid, nil
	}
}

// SignProposal signs an unsigned proposal and attach the signature to the signed proposal
func SignProposal(prop *peer.Proposal) (*peer.SignedProposal, error) {
	proposalBytes, err := proto.Marshal(prop)
	if err != nil {
		return nil, err
	}

	signature, err := config.Identity.Sign(proposalBytes)
	if err != nil {
		return nil, err
	}

	signedProposal := &peer.SignedProposal{
		ProposalBytes: proposalBytes,
		Signature:     signature,
	}
	return signedProposal, nil
}

// CreateSignedTx extract response, then signs and generates an envelope
func CreateSignedTx(proposal *peer.Proposal, responses []*peer.ProposalResponse) (*common.Envelope, error) {
	if len(responses) == 0 {
		return nil, errors.Errorf("Fail to find any response")
	}

	if config.CheckRWSet {
		mustPrintTXRWSet(responses)
	}

	header, err := getHeader(proposal.Header)
	if err != nil {
		return nil, err
	}

	ccActionPayload, err := generateChaincodeActionPayload(proposal, responses)
	if err != nil {
		return nil, err
	}

	tx, err := generateTransaction(header, ccActionPayload)
	if err != nil {
		return nil, err
	}

	payload, err := generatePayload(header, tx)
	if err != nil {
		return nil, err
	}

	return generateEnvelope(payload)
}

func CreateSignedDeliverNewestEnv() (*common.Envelope, error) {
	start := &orderer.SeekPosition{
		Type: &orderer.SeekPosition_Newest{
			Newest: &orderer.SeekNewest{},
		},
	}

	stop := &orderer.SeekPosition{
		Type: &orderer.SeekPosition_Specified{
			Specified: &orderer.SeekSpecified{
				Number: math.MaxUint64,
			},
		},
	}

	seekInfo := &orderer.SeekInfo{
		Start:    start,
		Stop:     stop,
		Behavior: orderer.SeekInfo_BLOCK_UNTIL_READY,
	}

	return protoutil.CreateSignedEnvelope(
		common.HeaderType_DELIVER_SEEK_INFO,
		config.Channel,
		config.Identity,
		seekInfo,
		0,
		0,
	)
}

func getHeader(headerBytes []byte) (*common.Header, error) {
	header := &common.Header{}
	err := proto.Unmarshal(headerBytes, header)
	if err != nil {
		return nil, errors.Wrap(err, "error unmarshaling Header")
	}

	err = checkHeaderSignerValidity(header)
	if err != nil {
		return nil, err
	}

	return header, nil
}

// checkHeaderSignerValidity check that the signer is the same
// that is referenced in the header.
func checkHeaderSignerValidity(header *common.Header) error {
	identityBytes, err := config.Identity.Serialize()
	if err != nil {
		return err
	}

	signatureHeader, err := GetSignatureHeader(header.SignatureHeader)
	if err != nil {
		return err
	}

	if bytes.Compare(identityBytes, signatureHeader.Creator) != 0 {
		return errors.Errorf("signer must be the same as the one referenced in the header")
	}

	return nil
}

func GetSignatureHeader(signatureHeaderBytes []byte) (*common.SignatureHeader, error) {
	return UnmarshalSignatureHeader(signatureHeaderBytes)
}

func UnmarshalSignatureHeader(bytes []byte) (*common.SignatureHeader, error) {
	sh := &common.SignatureHeader{}
	if err := proto.Unmarshal(bytes, sh); err != nil {
		return nil, errors.Wrap(err, "error unmarshaling SignatureHeader")
	}
	return sh, nil
}

func collectEndorsements(responses []*peer.ProposalResponse) ([]*peer.Endorsement, error) {
	err := checkResponsesStatusValidity(responses)
	if err != nil {
		return nil, err
	}

	err = checkResponsePayloadValidity(responses)
	if err != nil {
		return nil, err
	}

	endorsements := make([]*peer.Endorsement, len(responses))
	for i, r := range responses {
		endorsements[i] = r.Endorsement
	}
	return endorsements, nil
}

func checkResponsesStatusValidity(responses []*peer.ProposalResponse) error {
	for _, r := range responses {
		if r.Response.Status < 200 || r.Response.Status >= 400 {
			return errors.Errorf("proposal response was not successful, error code %d, msg %s", r.Response.Status, r.Response.Message)
		}
	}
	return nil
}

func checkResponsePayloadValidity(responses []*peer.ProposalResponse) error {
	payloadBytes := getProposalResponsePayloadByte(responses)
	for i, r := range responses {
		if i == 0 {
			continue
		}
		if bytes.Compare(payloadBytes, r.Payload) != 0 {
			return errors.Errorf("ProposalResponsePayloads from Peers do not match")
		}
	}
	return nil
}

func getProposalResponsePayloadByte(responses []*peer.ProposalResponse) []byte {
	return responses[0].Payload
}

func GetChaincodeProposalPayload(ccProposalPayloadBytes []byte) (*peer.ChaincodeProposalPayload, error) {
	ccProposalPayload := &peer.ChaincodeProposalPayload{}
	err := proto.Unmarshal(ccProposalPayloadBytes, ccProposalPayload)
	return ccProposalPayload, errors.Wrap(err, "error unmarshaling ChaincodeProposalPayload")
}

func mustPrintTXRWSet(responses []*peer.ProposalResponse) {
	proposalResponsePayloadByte := getProposalResponsePayloadByte(responses)
	proposalResponsePayload, err := protoutil.UnmarshalProposalResponsePayload(proposalResponsePayloadByte)
	if err != nil {
		logger.Errorf("Fail to unmarshal ProposalResponsePayload: %v", err)
	}

	ccAction, err := protoutil.UnmarshalChaincodeAction(proposalResponsePayload.Extension)
	if err != nil {
		logger.Errorf("Fail to unmarshal ChaincodeAction: %v", err)
	}

	txRWSet := &rwsetutil.TxRwSet{}
	err = txRWSet.FromProtoBytes(ccAction.Results)
	if err != nil {
		if err != nil {
			logger.Errorf("Fail to deserializes protobytes into TxReadWriteSet proto message: %v", err)
		}
	}

	for _, rwset := range txRWSet.NsRwSets {
		fmt.Println("Namespace:", rwset.NameSpace)

		fmt.Println("Read Set")
		for _, rset := range rwset.KvRwSet.Reads {
			fmt.Println(rset.String())
		}

		fmt.Println("Write Set")
		for _, wset := range rwset.KvRwSet.Writes {
			fmt.Println(wset.String())
		}
	}
}

func generateChaincodeActionPayload(proposal *peer.Proposal, responses []*peer.ProposalResponse) (*peer.ChaincodeActionPayload, error) {
	ccProposalPayload, err := GetChaincodeProposalPayload(proposal.Payload)
	if err != nil {
		return nil, err
	}
	proposalPayloadBytes, err := protoutil.GetBytesProposalPayloadForTx(ccProposalPayload)
	if err != nil {
		return nil, err
	}

	endorsements, err := collectEndorsements(responses)
	if err != nil {
		return nil, err
	}

	ccEndorsedAction := &peer.ChaincodeEndorsedAction{
		ProposalResponsePayload: getProposalResponsePayloadByte(responses),
		Endorsements:            endorsements,
	}

	ccActionPayload := &peer.ChaincodeActionPayload{
		ChaincodeProposalPayload: proposalPayloadBytes,
		Action:                   ccEndorsedAction,
	}
	return ccActionPayload, nil
}

func generateTransaction(header *common.Header, ccActionPayload *peer.ChaincodeActionPayload) (*peer.Transaction, error) {
	ccActionPayloadBytes, err := protoutil.GetBytesChaincodeActionPayload(ccActionPayload)
	if err != nil {
		return nil, err
	}

	// create a transaction
	txAction := &peer.TransactionAction{
		Header:  header.SignatureHeader,
		Payload: ccActionPayloadBytes,
	}
	txActions := make([]*peer.TransactionAction, 1)
	txActions[0] = txAction

	tx := &peer.Transaction{Actions: txActions}
	return tx, nil
}

func generatePayload(header *common.Header, tx *peer.Transaction) (*common.Payload, error) {
	txBytes, err := protoutil.GetBytesTransaction(tx)
	if err != nil {
		return nil, err
	}

	// create the payload
	payload := &common.Payload{
		Header: header,
		Data:   txBytes,
	}

	return payload, nil
}

func generateEnvelope(payload *common.Payload) (*common.Envelope, error) {
	payloadBytes, err := protoutil.GetBytesPayload(payload)
	if err != nil {
		return nil, err
	}

	signature, err := config.Identity.Sign(payloadBytes)
	if err != nil {
		return nil, err
	}

	envelope := &common.Envelope{
		Payload:   payloadBytes,
		Signature: signature,
	}
	return envelope, nil
}

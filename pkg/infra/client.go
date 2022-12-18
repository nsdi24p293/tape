package infra

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/osdi23p228/fabric-protos-go/orderer"
	"github.com/osdi23p228/fabric-protos-go/peer"
	"github.com/osdi23p228/tape/pkg/comm"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const (
	MAX_TRY = 3
)

func newGRPCClient(node Node) (*comm.GRPCClient, error) {
	clientConfig := generateClientConfig(node)

	grpcClient, err := comm.NewGRPCClient(clientConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "error connecting to %s", node.Address)
	}

	return grpcClient, nil
}

func generateClientConfig(node Node) comm.ClientConfig {
	certs := collectTLSCACertsBytes(node)

	clientConfig := comm.ClientConfig{
		Timeout: 30 * time.Second,
		SecOpts: comm.SecureOptions{
			UseTLS:            false,
			RequireClientCert: false,
			ServerRootCAs:     certs,
		},
	}

	if len(certs) > 0 {
		clientConfig.SecOpts.UseTLS = true
		if len(node.TLSCAKey) > 0 && len(node.TLSCARoot) > 0 {
			clientConfig.SecOpts.RequireClientCert = true
			clientConfig.SecOpts.Certificate = node.TLSCACertByte
			clientConfig.SecOpts.Key = node.TLSCAKeyByte
			if node.TLSCARootByte != nil {
				clientConfig.SecOpts.ClientRootCAs = append(clientConfig.SecOpts.ClientRootCAs, node.TLSCARootByte)
			}
		}
	}

	return clientConfig
}

func collectTLSCACertsBytes(node Node) [][]byte {
	var certs [][]byte
	if node.TLSCACertByte != nil {
		certs = append(certs, node.TLSCACertByte)
	}
	return certs
}

func CreateEndorserClient(node Node) (peer.EndorserClient, error) {
	conn, err := DialConnection(node)
	if err != nil {
		return nil, err
	}
	return peer.NewEndorserClient(conn), nil
}

func CreateBroadcastClient(node Node) (orderer.AtomicBroadcast_BroadcastClient, error) {
	conn, err := DialConnection(node)
	if err != nil {
		return nil, err
	}
	return orderer.NewAtomicBroadcastClient(conn).Broadcast(context.Background())
}

func CreateDeliverFilteredClient() (peer.Deliver_DeliverFilteredClient, error) {
	conn, err := DialConnection(config.Committer)
	if err != nil {
		return nil, err
	}
	return peer.NewDeliverClient(conn).DeliverFiltered(context.Background())
}

func DialConnection(node Node) (*grpc.ClientConn, error) {
	gRPCClient, err := newGRPCClient(node)
	if err != nil {
		return nil, err
	}

	for i := 1; i <= MAX_TRY; i++ {
		conn, err := gRPCClient.NewConnection(
			node.Address,
			func(tlsConfig *tls.Config) {
				tlsConfig.InsecureSkipVerify = true
			},
		)
		if err == nil {
			return conn, nil
		}
	}
	return nil, errors.Wrapf(err, "failed to dial %s: %s", node.Address)
}

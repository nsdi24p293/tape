package infra

import (
	"fmt"
	"io/ioutil"

	"github.com/osdi23p228/fabric-protos-go/msp"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

var (
	itemNotProvidedError = errors.New("No such item")
)

type Node struct {
	Address       string `yaml:"address"`
	TLSCACert     string `yaml:"tlsCACert"`
	TLSCAKey      string `yaml:"tlsCAKey"`
	TLSCARoot     string `yaml:"tlsCARoot"`
	TLSCACertByte []byte
	TLSCAKeyByte  []byte
	TLSCARootByte []byte
}

type Config struct {
	// Network
	Endorsers []Node `yaml:"endorsers"` // peers
	Committer Node   `yaml:"committer"` // the peer chosen to observe blocks from
	Orderer   Node   `yaml:"orderer"`   // orderer
	Channel   string `yaml:"channel"`   // name of the channel to be operated on

	// Chaincode
	Chaincode string   `yaml:"chaincode"` // chaincode name
	Version   string   `yaml:"version"`   // chaincode version
	Args      []string `yaml:"args"`      // chaincode arguments

	// Client identity
	MSPID      string  `yaml:"mspid"`      // the MSP the client belongs
	PrivateKey string  `yaml:"privateKey"` // client's private key
	SignCert   string  `yaml:"signCert"`   // client's certificate
	Identity   *Crypto // client's identity

	End2End bool `yaml:"e2e"` // running mode

	Rate  int `yaml:"rate"`  // average speed of transaction generation
	Burst int `yaml:"burst"` // maximum speed of transaction generation

	TxNum           int     `yaml:"txNum"`           // number of transactions
	TxTime          int     `yaml:"txTime"`          // maximum execution time
	TxType          string  `yaml:"txType"`          // transaction type ['put', 'conflict']
	TxIDStart       int     `yaml:"txIDStart"`       // the start of TX ID
	Session         string  `yaml:"session"`         // session name
	HotAccountRatio float64 `yaml:"hotAccountRatio"` // percentage of hot accounts
	ConflictRatio   float64 `yaml:"conflictRatio"`   // Percentage of conflict

	ConnNum          int `yaml:"connNum"`          // number of connection
	ClientPerConnNum int `yaml:"clientPerConnNum"` // number of client per connection
	IntegratorNum    int `yaml:"integratorNum"`    // number of integrator
	BroadcasterNum   int `yaml:"broadcasterNum"`   // number of orderer client
	EndorserNum      int // number of endorsers

	// If true, let the protoutil generate txid automatically
	// If false, encode the txid by us
	// WARNING: Must modify the code in core/endorser/msgvalidation.go:Validate() and
	// protoutil/proputils.go:ComputeTxID (v2) before compilation
	CheckTxID bool `yaml:"checkTxID"`

	// If true, print the read set and write set to STDOUT
	CheckRWSet bool `yaml:"checkRWSet"`

	LogPath    string `yaml:"logPath"`    // path of the log file
	ReportPath string `yaml:"reportPath"` // path of the report file

	Seed int `yaml:"seed"` // random seed
}

func (c *Config) mustLoadRawConfigFromFile(filename string) {
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		logger.Panicf("Fail to load %s: %v", filename, err)
	}

	err = yaml.Unmarshal(raw, c)
	if err != nil {
		logger.Panicf("Fail to unmarshal %s: %v", filename, err)
	}
}

func (c *Config) mustLoadEndorserConfig() {
	for i := range c.Endorsers {
		c.Endorsers[i].mustLoadConfig()
	}
	c.EndorserNum = len(c.Endorsers)
}

func (c *Config) mustLoadCommiterConfig() {
	c.Committer.mustLoadConfig()
}

func (c *Config) mustLoadOrdererConfig() {
	c.Orderer.mustLoadConfig()
}

func (c *Config) mustValid() {
	if c.Rate < 0 {
		logger.Panicf("Rate %f is not a zero (unlimited) or positive number\n", c.Rate)
	}

	if c.Burst < 1 {
		logger.Panicf("Burst %d is not greater than 1\n", c.Burst)
	}

	if c.Rate > c.Burst {
		logger.Printf("Rate %d is bigger than burst %d, so let rate equal to burst\n", c.Rate, c.Burst)
		c.Rate = c.Burst
	}

	if c.ConflictRatio < 0 || c.ConflictRatio > 1 {
		logger.Panicf("Conflict ratio %f is not within the range of [0, 1]\n", c.ConflictRatio)
	}

	if c.HotAccountRatio < 0 || c.HotAccountRatio > 1 {
		logger.Panicf("Hot account ratio %f is not within the range of [0, 1]\n", c.HotAccountRatio)
	}

	fmt.Printf("Conflict ratio %f\n", c.ConflictRatio)
	fmt.Printf("Hot account ratio %f\n", c.HotAccountRatio)
}

func LoadConfigFromFile(filename string) (*Config, error) {
	c := &Config{}

	c.mustLoadRawConfigFromFile(filename)
	c.mustLoadEndorserConfig()
	c.mustLoadCommiterConfig()
	c.mustLoadOrdererConfig()
	c.mustLoadClientIdentity()

	c.mustValid()

	return c, nil
}

// mustLoadClientIdentity loads the client specified in the configuration file
func (c *Config) mustLoadClientIdentity() {
	cc := CryptoConfig{
		MSPID:    c.MSPID,
		PrivKey:  c.PrivateKey,
		SignCert: c.SignCert,
	}

	privateKey, err := GetPrivateKey(cc.PrivKey)
	if err != nil {
		logger.Fatalf("Fail to load private key: %v", err)
	}

	cert, certBytes, err := GetCertificate(cc.SignCert)
	if err != nil {
		logger.Fatalf("Fail to load certificate: %v", err)
	}

	id := &msp.SerializedIdentity{
		Mspid:   cc.MSPID,
		IdBytes: certBytes,
	}
	name, err := proto.Marshal(id)
	if err != nil {
		logger.Fatalf("Fail to get msp id: %v", err)
	}

	c.Identity = &Crypto{
		Creator:  name,
		PrivKey:  privateKey,
		SignCert: cert,
	}
}

func GetTLSCACerts(file string) ([]byte, error) {
	if file == "" {
		return nil, itemNotProvidedError
	}

	in, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to load %s", file)
	}

	return in, nil
}

// TODO
func (n *Node) mustLoadConfig() {
	certByte, err := GetTLSCACerts(n.TLSCACert)
	if err != nil && err != itemNotProvidedError {
		logger.Fatalf("Fail to load TLS CA Cert %s: %v", n.TLSCACert, err)
	}

	keyByte, err := GetTLSCACerts(n.TLSCAKey)
	if err != nil && err != itemNotProvidedError {
		logger.Fatalf("Fail to load TLS CA Key %s: %v", n.TLSCAKey, err)
	}

	rootByte, err := GetTLSCACerts(n.TLSCARoot)
	if err != nil && err != itemNotProvidedError {
		logger.Fatalf("Fail to load TLS CA Root %s: %v", n.TLSCARoot, err)
	}

	n.TLSCACertByte = certByte
	n.TLSCAKeyByte = keyByte
	n.TLSCARootByte = rootByte
}

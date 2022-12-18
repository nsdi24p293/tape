package infra

import (
	"bufio"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	accountFilePath     = "ACCOUNTS.txt"
	transactionFilePath = "TRANSACTIONS.txt"
)

var (
	chs = []rune("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890!@#$%^&*()=")
)

type WorkloadGenerator struct {
	ccArgsList [][]string
	accounts   []string
}

func generateCCArgsList() [][]string {
	initSeed()
	wg := NewWorkloadGenerator()

	for i := 0; i < config.TxNum; i++ {
		wg.ccArgsList[i] = wg.generateCCArgs()
	}

	wg.mustWriteArgsToFile()
	if config.TxType == "put" {
		wg.mustWriteAccountsToFile()
	}

	return wg.ccArgsList
}

func initSeed() {
	if config.Seed == 0 {
		rand.Seed(time.Now().UnixNano())
	} else {
		rand.Seed(int64(config.Seed))
	}
}

func NewWorkloadGenerator() *WorkloadGenerator {
	wg := &WorkloadGenerator{
		ccArgsList: make([][]string, config.TxNum),
	}

	if config.TxType == "conflict" {
		wg.mustLoadAccountsFromFile()
	}

	return wg
}

func (wg *WorkloadGenerator) mustLoadAccountsFromFile() {
	// try to load all accounts' id from file
	if _, err := os.Stat(accountFilePath); os.IsNotExist(err) {
		logger.Fatalf("Fail to find account file %s: %v\n", accountFilePath, err)
	}

	af, err := os.Open(accountFilePath)
	if err != nil {
		logger.Fatalf("Fail to open account file %s: %v\n", accountFilePath, err)
	}
	defer af.Close()

	input := bufio.NewScanner(af)
	for input.Scan() {
		accountID := input.Text()
		wg.accounts = append(wg.accounts, accountID)
	}
	logger.Infof("Load %d accounts from %s", len(wg.accounts), accountFilePath)
}

func (wg *WorkloadGenerator) generateCCArgs() []string {
	switch config.TxType {
	case "put":
		return wg.generateCCArgsPut()
	case "conflict":
		return wg.generateCCArgsConflict()
	default:
		return nil
	}
}

func (wg *WorkloadGenerator) generateCCArgsPut() []string {
	var result []string

	id := getName(64) // generate a random name for customer

	result = append(result, "CreateAccount")   // function name
	result = append(result, id)                // customer id
	result = append(result, id)                // customer name
	result = append(result, strconv.Itoa(1e9)) // savings balance
	result = append(result, strconv.Itoa(1e9)) // checking balance

	return result
}

func getName(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = chs[rand.Intn(len(chs))]
	}
	return string(b)
}

func (wg *WorkloadGenerator) generateCCArgsConflict() []string {
	var result []string

	senderName, receiverName := wg.selectTwoDifferentAccounts()

	result = append(result, "SendPayment") // function name
	result = append(result, senderName)    // sender name
	result = append(result, receiverName)  // receiver name
	result = append(result, "1")           // amount

	return result
}

func (wg *WorkloadGenerator) selectTwoDifferentAccounts() (string, string) {
	senderName := wg.selectAccount()
	receiverName := wg.selectAccount()
	for senderName == receiverName {
		receiverName = wg.selectAccount()
	}

	return senderName, receiverName
}

func (wg *WorkloadGenerator) selectAccount() string {
	randomNumber := rand.Float64()
	if randomNumber < config.ConflictRatio {
		return wg.selectHotAccount()
	} else {
		return wg.selectColdAccount()
	}
}

func (wg *WorkloadGenerator) selectHotAccount() string {
	hotAccountNumber := int(config.HotAccountRatio * float64(len(wg.accounts)))
	accountID := rand.Intn(hotAccountNumber)
	accountName := wg.accounts[accountID]
	return accountName
}

func (wg *WorkloadGenerator) selectColdAccount() string {
	hotAccountNumber := int(config.HotAccountRatio * float64(len(wg.accounts)))
	coldAccountNumber := len(wg.accounts) - hotAccountNumber
	accountID := rand.Intn(coldAccountNumber) + hotAccountNumber
	accountName := wg.accounts[accountID]
	return accountName
}

func (wg *WorkloadGenerator) mustWriteArgsToFile() {
	os.Remove(transactionFilePath)

	tf, err := os.Create(transactionFilePath)
	if err != nil {
		logger.Fatalf("Failed to create file %s: %v\n", transactionFilePath, err)
	}
	defer tf.Close()

	for i := 0; i < config.TxNum; i++ {
		tf.WriteString(strconv.Itoa(i) + " " + strings.Join(wg.ccArgsList[i], " ") + "\n")
	}
}

func (wg *WorkloadGenerator) mustWriteAccountsToFile() {
	af, err := os.Create(accountFilePath)
	defer af.Close()
	if err != nil {
		logger.Fatalf("Failed to create file %s: %v\n", accountFilePath, err)
	}
	for i := 0; i < config.TxNum; i++ {
		// only record the account id
		af.WriteString(wg.ccArgsList[i][1] + "\n")
	}
}

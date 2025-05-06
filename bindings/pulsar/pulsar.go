package pulsar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/common/authentication/oauth2"
	"github.com/dapr/kit/logger"
)

const (
	host                    = "host"
	consumerID              = "consumerID"
	enableTLS               = "enableTLS"
	deliverAt               = "deliverAt"
	deliverAfter            = "deliverAfter"
	disableBatching         = "disableBatching"
	batchingMaxPublishDelay = "batchingMaxPublishDelay"
	batchingMaxSize         = "batchingMaxSize"
	batchingMaxMessages     = "batchingMaxMessages"
	tenant                  = "tenant"
	namespace               = "namespace"
	persistent              = "persistent"
	redeliveryDelay         = "redeliveryDelay"
	avroProtocol            = "avro"
	jsonProtocol            = "json"
	protoProtocol           = "proto"
	partitionKey            = "partitionKey"

	defaultTenant     = "public"
	defaultNamespace  = "default"
	cachedNumProducer = 10
	pulsarPrefix      = "pulsar://"
	pulsarToken       = "token"
	// topicFormat is the format for pulsar, which have a well-defined structure: {persistent|non-persistent}://tenant/namespace/topic,
	// see https://pulsar.apache.org/docs/en/concepts-messaging/#topics for details.
	topicFormat                = "%s://%s/%s/%s"
	persistentStr              = "persistent"
	nonPersistentStr           = "non-persistent"
	topicJSONSchemaIdentifier  = ".jsonschema"
	topicAvroSchemaIdentifier  = ".avroschema"
	topicProtoSchemaIdentifier = ".protoschema"

	// defaultBatchingMaxPublishDelay init default for maximum delay to batch messages.
	defaultBatchingMaxPublishDelay = 10 * time.Millisecond
	// defaultMaxMessages init default num of entries in per batch.
	defaultMaxMessages = 1000
	// defaultMaxBatchSize init default for maximum number of bytes per batch.
	defaultMaxBatchSize = 128 * 1024
	// defaultRedeliveryDelay init default for redelivery delay.
	defaultRedeliveryDelay = 30 * time.Second
	// defaultConcurrency controls the number of concurrent messages sent to the app.
	defaultConcurrency = 100
	// defaultReceiverQueueSize controls the number of messages the pulsar sdk pulls before dapr explicitly consumes the messages.
	defaultReceiverQueueSize = 1000

	subscribeTypeKey = "subscribeType"

	subscribeTypeExclusive = "exclusive"
	subscribeTypeShared    = "shared"
	subscribeTypeFailover  = "failover"
	subscribeTypeKeyShared = "key_shared"

	processModeKey = "processMode"

	processModeAsync = "async"
	processModeSync  = "sync"

	subscribeInitialPosition = "subscribeInitialPosition"

	subscribePositionEarliest = "earliest"
	subscribePositionLatest   = "latest"
)

type ProcessMode string

type pulsarMetadata struct {
	Host                             string                    `mapstructure:"host"`
	ConsumerID                       string                    `mapstructure:"consumerID"`
	EnableTLS                        bool                      `mapstructure:"enableTLS"`
	DisableBatching                  bool                      `mapstructure:"disableBatching"`
	BatchingMaxPublishDelay          time.Duration             `mapstructure:"batchingMaxPublishDelay"`
	BatchingMaxSize                  uint                      `mapstructure:"batchingMaxSize"`
	BatchingMaxMessages              uint                      `mapstructure:"batchingMaxMessages"`
	Tenant                           string                    `mapstructure:"tenant"`
	Namespace                        string                    `mapstructure:"namespace"`
	Persistent                       bool                      `mapstructure:"persistent"`
	RedeliveryDelay                  time.Duration             `mapstructure:"redeliveryDelay"`
	internalTopicSchemas             map[string]schemaMetadata `mapstructure:"-"`
	PublicKey                        string                    `mapstructure:"publicKey"`
	PrivateKey                       string                    `mapstructure:"privateKey"`
	Keys                             string                    `mapstructure:"keys"`
	MaxConcurrentHandlers            uint                      `mapstructure:"maxConcurrentHandlers"`
	ReceiverQueueSize                int                       `mapstructure:"receiverQueueSize"`
	SubscriptionType                 string                    `mapstructure:"subscribeType"`
	SubscriptionInitialPosition      string                    `mapstructure:"subscribeInitialPosition"`
	Token                            string                    `mapstructure:"token"`
	oauth2.ClientCredentialsMetadata `mapstructure:",squash"`
}

type schemaMetadata struct {
	protocol string
	value    string
}

type Pulsar struct {
	logger   logger.Logger
	client   pulsar.Client
	metadata *pulsarMetadata
	closed   atomic.Bool
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

func NewPulsar(logger logger.Logger) bindings.InputOutputBinding {
	return &Pulsar{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

func (p *Pulsar) Init(ctx context.Context, metadata bindings.Metadata) error {
	m, err := p.parseMetadata(metadata)
	if err != nil {
		return err
	}
	var pulsarMeta pulsarMetadata
	err = json.Unmarshal(m, &pulsarMeta)
	if err != nil {
		return err
	}
	pulsarURL := pulsarMeta.Host
	pulsarURL = sanitiseURL(pulsarURL)

	options := pulsar.ClientOptions{
		URL:                        pulsarURL,
		OperationTimeout:           30 * time.Second,
		ConnectionTimeout:          30 * time.Second,
		TLSAllowInsecureConnection: !pulsarMeta.EnableTLS,
	}
	switch {
	case len(pulsarMeta.Token) > 0:
		options.Authentication = pulsar.NewAuthenticationToken(pulsarMeta.Token)
	case len(pulsarMeta.ClientCredentialsMetadata.TokenURL) > 0:
		var cc *oauth2.ClientCredentials
		cc, err = oauth2.NewClientCredentials(ctx, oauth2.ClientCredentialsOptions{
			Logger:       p.logger,
			TokenURL:     pulsarMeta.ClientCredentialsMetadata.TokenURL,
			CAPEM:        []byte(pulsarMeta.ClientCredentialsMetadata.TokenCAPEM),
			ClientID:     pulsarMeta.ClientCredentialsMetadata.ClientID,
			ClientSecret: pulsarMeta.ClientCredentialsMetadata.ClientSecret,
			Scopes:       pulsarMeta.ClientCredentialsMetadata.Scopes,
			Audiences:    pulsarMeta.ClientCredentialsMetadata.Audiences,
		})
		if err != nil {
			return fmt.Errorf("could not instantiate oauth2 token provider: %w", err)
		}

		options.Authentication = pulsar.NewAuthenticationTokenFromSupplier(cc.Token)
	}
	client, err := pulsar.NewClient(options)
	if err != nil {
		return fmt.Errorf("could not instantiate pulsar client: %v", err)
	}

	p.client = client
	p.metadata = &pulsarMeta

	return nil
}

func (p *Pulsar) Read(ctx context.Context, handler bindings.Handler) error {
	if p.closed.Load() {
		return errors.New("binding is closed")
	}
	p.wg.Add(1)
	return nil
}

func (p *Pulsar) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (p *Pulsar) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {

	return nil, err
}

func (p *Pulsar) Close() error {
	defer p.wg.Wait()
	if p.closed.CompareAndSwap(false, true) {
		close(p.closeCh)
	}
	p.client.Close()
	return nil
}

func (p *Pulsar) parseMetadata(metadata bindings.Metadata) ([]byte, error) {
	return json.Marshal(metadata.Properties)
}

func sanitiseURL(pulsarURL string) string {
	prefixes := []string{"pulsar+ssl://", "pulsar://", "http://", "https://"}

	hasPrefix := false
	for _, prefix := range prefixes {
		if strings.HasPrefix(pulsarURL, prefix) {
			hasPrefix = true
			break
		}
	}

	if !hasPrefix {
		pulsarURL = fmt.Sprintf("%s%s", pulsarPrefix, pulsarURL)
	}
	return pulsarURL
}

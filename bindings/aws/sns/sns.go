/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sns

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/service/sns"

	"github.com/dapr/components-contrib/bindings"
	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// AWSSNS is an AWS SNS binding.
type AWSSNS struct {
	authProvider awsAuth.Provider
	topicARN     string

	logger logger.Logger
}

type snsMetadata struct {
	// Ignored by metadata parser because included in built-in authentication profile
	AccessKey    string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	SecretKey    string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken" mdignore:"true"`

	TopicArn string `json:"topicArn"`
	// TODO: in Dapr 1.17 rm the alias on region as we remove the aws prefix on these fields
	Region   string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion" mdignore:"true"`
	Endpoint string `json:"endpoint"`
}

type dataPayload struct {
	Message interface{} `json:"message"`
	Subject interface{} `json:"subject"`
}

// NewAWSSNS creates a new AWSSNS binding instance.
func NewAWSSNS(logger logger.Logger) bindings.OutputBinding {
	return &AWSSNS{logger: logger}
}

// Init does metadata parsing.
func (a *AWSSNS) Init(ctx context.Context, metadata bindings.Metadata) error {
	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}

	opts := awsAuth.Options{
		Logger:       a.logger,
		Properties:   metadata.Properties,
		Region:       m.Region,
		Endpoint:     m.Endpoint,
		AccessKey:    m.AccessKey,
		SecretKey:    m.SecretKey,
		SessionToken: m.SessionToken,
	}
	// extra configs needed per component type
	provider, err := awsAuth.NewProvider(ctx, opts, awsAuth.GetConfig(opts))
	if err != nil {
		return err
	}
	a.authProvider = provider
	a.topicARN = m.TopicArn

	return nil
}

func (a *AWSSNS) parseMetadata(meta bindings.Metadata) (*snsMetadata, error) {
	m := snsMetadata{}
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (a *AWSSNS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AWSSNS) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload dataPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	msg := fmt.Sprintf("%v", payload.Message)
	subject := fmt.Sprintf("%v", payload.Subject)

	_, err = a.authProvider.Sns().Sns.PublishWithContext(ctx, &sns.PublishInput{
		Message:  &msg,
		Subject:  &subject,
		TopicArn: &a.topicARN,
	})
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// GetComponentMetadata returns the metadata of the component.
func (a *AWSSNS) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := snsMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}

func (a *AWSSNS) Close() error {
	if a.authProvider != nil {
		return a.authProvider.Close()
	}
	return nil
}

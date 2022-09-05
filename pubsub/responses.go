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

package pubsub

// AppResponseStatus represents a status of a PubSub response.
type AppResponseStatus string

type BatchPublishStatus string

const (
	// Success means the message is received and processed correctly.
	Success AppResponseStatus = "SUCCESS"
	// Retry means the message is received but could not be processed and must be retried.
	Retry AppResponseStatus = "RETRY"
	// Drop means the message is received but should not be processed.
	Drop AppResponseStatus = "DROP"
	// Represents that message was published successfully.
	PublishSuccess BatchPublishStatus = "SUCCESS"
	// Represents that message publishing Failed.
	PublishFailed BatchPublishStatus = "FAILED"
)

// AppResponse is the object describing the response from user code after a pubsub event.
type AppResponse struct {
	Status AppResponseStatus `json:"status"`
}

type AppResponseItem struct {
	EventId string            `json:"eventId"`
	Status  AppResponseStatus `json:"status"`
}

type AppBatchResponse struct {
	AppResponses []AppResponseItem `json:"statuses"`
}

type BatchPublishResponseItem struct {
	EventId string             `json:"eventId"`
	Status  BatchPublishStatus `json:"status"`
}

type BatchPublishResponse struct {
	Error    error                      `json:"error"`
	Statuses []BatchPublishResponseItem `json:"statuses"`
}

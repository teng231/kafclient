# v1.1.1
- Add interface struct `IPubsub` define all methods.
- Depredicate `InitConsumer` and `OnScanMessages` will be remove some version.
- Add `PublishWithConfig` to push message with multiple config. Now can add header, metadata.
PublishWithConfig: help we can send message to a partition to process sync task.
- struct `Topic` add atrribute Partition to manual setup send message to a partition.
```go
type Topic struct {
	Name       string
	AutoCommit bool
	Partition  *int32
}
```
- struct `Message` add attribute Header
``` go
type Message struct {
	Offset        int64  `json:"offset,omitempty"`
	Partition     int    `json:"partition,omitempty"`
	Topic         string `json:"topic,omitempty"`
	Body          []byte `json:"body,omitempty"`
	Timestamp     int64  `json:"timestamp,omitempty"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
	Commit        func()
	// new property
	Headers map[string]string
}
```
- Add struct `SenderConfig` help add more information about sender.
```go
// SenderConfig addion config when publish message
type SenderConfig struct {
	Metadata interface{}
	Headers  map[string]string
}
```
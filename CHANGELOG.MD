# v1.2.4 BIG Update
- hotfix issue kafka of sarama
# v1.2.3 BIG Update
- Change struct name `PubSub` to `Client`
- Change interface IPubSub to IClient
# v1.1.6
- Add Async publish, send message async
- Update sarama. because v1.31.1 publisher deadlock
# v1.1.5
- Add manual for create topic or not
- config at `config/server.properties` with config `auto.create.topics.enable`
# v1.1.4
- Upgrade code, fix issue create topic
- update golang version
# v1.1.3
- Update struct name, syntax, remove log
# v1.1.2
- Update README
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
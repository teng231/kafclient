module github.com/teng231/kafclient/v2

go 1.20

require github.com/segmentio/kafka-go v0.4.40

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)

// retract (
// 	v2.0.5 // [slow] slow for listener
// )
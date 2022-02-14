publisher-simple:
	go test --run=TestPuslishMessages
subscribe-simple:
	CG=group-0 go test --run=TestSubscribeSimpleManualCommit
pubsub-simple:
	CG=group-0 go test --run=TestSimplePublishAndSubscibe
benchmark-publish-100:
	go test -benchmem -bench=BenchmarkPublishMessages100z -run=^a
benchmark-publish-10000:
	go test -benchmem -bench=BenchmarkPublishMessages10000z -run=^a

benchmark-subscribe-10000:
	go test -benchmem -bench=BenchmarkSubscribeSimpleManualCommit -run=^a
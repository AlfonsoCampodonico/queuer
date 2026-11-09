module github.com/AlfonsoCampodonico/queuer/sqs

go 1.25.0

require (
	github.com/AlfonsoCampodonico/queuer v0.2.0
	github.com/aws/aws-sdk-go-v2 v1.47.7
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.10
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.10 // indirect
	github.com/aws/smithy-go v1.28.8 // indirect
)

// Development against the core module in this repository. Ignored by
// consumers, who resolve the tagged version above.
replace github.com/AlfonsoCampodonico/queuer => ../

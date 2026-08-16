module github.com/salribaudo/ledger/kafka

go 1.22

require (
	github.com/lib/pq v1.12.3
	github.com/salribaudo/ledger v0.0.0
	github.com/segmentio/kafka-go v0.4.47
)

replace github.com/salribaudo/ledger => ../

# `bully-election`

[![MIT License](https://img.shields.io/github/license/octu0/bully-election)](https://github.com/octu0/bully-election/blob/master/LICENSE)
[![GoDoc](https://pkg.go.dev/badge/github.com/octu0/bully-election)](https://pkg.go.dev/github.com/octu0/bully-election)
[![Go Report Card](https://goreportcard.com/badge/github.com/octu0/bully-election)](https://goreportcard.com/report/github.com/octu0/bully-election)
[![Releases](https://img.shields.io/github/v/release/octu0/bully-election)](https://github.com/octu0/bully-election/releases)

[memberlist](https://github.com/hashicorp/memberlist) based [Bully Election](https://en.wikipedia.org/wiki/Bully_algorithm).

this implementation contains `Voter / Novoter` so the leader election status can be retrieved without including it in the cluster.

## Installation

```bash
go get github.com/octu0/bully-election
```

## Example

```go
import (
	"context"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/octu0/bully-election"
)

func main() {
	ctx := context.Background()
	conf := memberlist.DefaultLocalConfig()
	conf.Name = "node1"
	conf.BindPort = 7947
	conf.AdvertiseAddr = "10.0.0.123"
	conf.AdvertisePort = conf.BindPort

	b, err := CreateVoter(ctx, conf,
		WithElectionTimeout(1*time.Second),
		WithObserveFunc(func(b *Bully, evt NodeEvent) {
			log.Printf("evt=%s", evt)
		}),
		WithOnErrorFunc(func(err error) {
			log.Printf("error=%+v", err)
		}),
	)
	err := b.Join("10.0.0.1")
	b.IsLeader()
	b.Leave()
}
```

# License

MIT, see LICENSE file for details.
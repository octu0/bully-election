# `bully-election`

[![MIT License](https://img.shields.io/github/license/octu0/bully-election)](https://github.com/octu0/bully-election/blob/master/LICENSE)
[![GoDoc](https://pkg.go.dev/badge/github.com/octu0/bully-election)](https://pkg.go.dev/github.com/octu0/bully-election)
[![Go Report Card](https://goreportcard.com/badge/github.com/octu0/bully-election)](https://goreportcard.com/report/github.com/octu0/bully-election)
[![Releases](https://img.shields.io/github/v/release/octu0/bully-election)](https://github.com/octu0/bully-election/releases)

Hashicorp's [memberlist](https://github.com/hashicorp/memberlist) based [Bully Leader Election](https://en.wikipedia.org/wiki/Bully_algorithm).

Features:
- Simple API
- Voter / Nonvoter node state management(monitoring)
- TransferLeadership

## Installation

```bash
go get github.com/octu0/bully-election
```

## Example

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/octu0/bully-election"
)

func main() {
	ctx := context.Background()
	conf := memberlist.DefaultLANConfig()
	conf.Name = "node1"
	conf.BindPort = 7947
	conf.AdvertiseAddr = "10.0.0.123"
	conf.AdvertisePort = conf.BindPort

	b, err := bullyelection.CreateVoter(ctx, conf,
		WithElectionTimeout(1*time.Second),
		WithObserveFunc(func(b *bullyelection.Bully, evt bullyelection.NodeEvent, id, addr string) {
			log.Printf("[%s] event: %s node=%s(%s)", b.ID(), evt.String(), id, addr)
			for _, n := range b.Members() {
				log.Printf("%s is_leader=%v", n.ID(), n.IsLeader())
			}
		}),
		WithOnErrorFunc(func(err error) {
			log.Printf("error=%+v", err)
		}),
	)
	err := b.Join("10.0.0.1")
	b.IsLeader()
	b.UpdateMetadata([]byte("hello world"))

	nn, _ := bullyelection.CreateNonVoter(ctx, conf2)
	err := nn.Join("10.0.0.1")
	for _, m := range nn.Members() {
		_ = m.UserMetadata()
	}

	b.Leave()
}
```

# License

MIT, see LICENSE file for details.

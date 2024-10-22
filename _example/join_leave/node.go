package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/hashicorp/memberlist"
	bullyelection "github.com/octu0/bully-election"
)

func main() {
	var (
		id   = flag.String("id", "node1", "node id")
		addr = flag.String("addr", "127.0.0.1", "ip addr")
		port = flag.Int("port", 7234, "port")
		join = flag.String("join", "127.0.0.1:7234", "join addr")
	)
	flag.Parse()

	conf := memberlist.DefaultLANConfig()
	conf.Name = *id
	conf.BindAddr = *addr
	conf.BindPort = *port
	conf.AdvertiseAddr = conf.BindAddr
	conf.AdvertisePort = conf.BindPort
	conf.EnableCompression = false

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig, sigStop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigStop()

	b, err := bullyelection.CreateVoter(ctx, conf, bullyelection.WithObserveFunc(func(b *bullyelection.Bully, evt bullyelection.NodeEvent) {
		log.Printf("[%s] event: %s", b.ID(), evt.String())
	}))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("startup")

	log.Printf("join: %s", *join)
	if err := b.Join(*join); err != nil {
		log.Fatal(err)
	}

	<-sig.Done()

	log.Printf("leave")
	if err := b.Leave(); err != nil {
		log.Fatal(err)
	}

	log.Println("bye")
}

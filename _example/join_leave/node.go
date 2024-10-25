package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

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

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	sig, sigStop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer sigStop()

	b, err := bullyelection.CreateVoter(ctx, conf, bullyelection.WithObserveFunc(func(b *bullyelection.Bully, evt bullyelection.NodeEvent, id, addr string) {
		log.Printf("[%s] event: %s node=%s(%s)", b.ID(), evt.String(), id, addr)
		if evt == bullyelection.ElectionEvent {
			for _, n := range b.Members() {
				log.Printf("%s is_leader=%v", n.ID(), n.IsLeader())
			}
		}
	}))
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("join: %s", *join)
	if err := b.Join(*join); err != nil {
		log.Fatal(err)
	}
	log.Printf("joined")

	<-sig.Done()

	log.Printf("leave")
	if err := b.Leave(); err != nil {
		log.Fatal(err)
	}

	log.Println("bye")
}

package bullyelection

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
)

func TestCreateVoter(t *testing.T) {
	t.Run("single_node", func(tt *testing.T) {
		conf := memberlist.DefaultLocalConfig()
		conf.Name = "test1"
		conf.BindAddr = "127.0.0.1"
		conf.BindPort = 0
		conf.AdvertiseAddr = "127.0.0.1"
		conf.AdvertisePort = 0

		ctx, cancel := context.WithCancel(context.Background())
		b, err := CreateVoter(ctx, conf,
			WithElectionTimeout(1*time.Second),
			WithObserveFunc(func(b *Bully, evt NodeEvent) {
				tt.Logf("evt=%s", evt)
			}),
			WithOnErrorFunc(func(err error) {
				tt.Fatalf("on error=%+v", err)
				cancel()
			}),
		)
		if err != nil {
			tt.Fatalf("CreateVoter: %+v", err)
		}
		tt.Logf("addr=%v", b.Address())

		if b.IsVoter() != true {
			tt.Errorf("must voter node")
		}

		if err := b.Join(b.Address()); err != nil {
			tt.Fatalf("Join: %+v", err)
		}
		tt.Logf("joined")

		if b.IsLeader() != true {
			tt.Errorf("single node = leader")
		}

		if err := b.Leave(); err != nil {
			tt.Fatalf("Leave: %+v", err)
		}
		tt.Logf("leaved")

		if err := b.Shutdown(); err != nil {
			tt.Fatalf("Shutdown: %+v", err)
		}
		tt.Logf("shutdown")
	})
}

/*
func TestCreateNonVoter(t *testing.T) {
	t.Parallel()
}*/

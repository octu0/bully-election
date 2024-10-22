package bullyelection

import (
	"bytes"
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
		conf.EnableCompression = false

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

	t.Run("two_node", func(tt *testing.T) {
		conf1 := memberlist.DefaultLocalConfig()
		conf1.Name = "test1"
		conf1.BindAddr = "127.0.0.1"
		conf1.BindPort = 0
		conf1.AdvertiseAddr = "127.0.0.1"
		conf1.AdvertisePort = 0
		conf1.EnableCompression = false

		conf2 := memberlist.DefaultLocalConfig()
		conf2.Name = "test2"
		conf2.BindAddr = "127.0.0.1"
		conf2.BindPort = 0
		conf2.AdvertiseAddr = "127.0.0.1"
		conf2.AdvertisePort = 0
		conf2.EnableCompression = false

		ctx, cancel := context.WithCancel(context.Background())
		b1, err := CreateVoter(ctx, conf1,
			WithElectionTimeout(1*time.Second),
			WithObserveFunc(func(b *Bully, evt NodeEvent) {
				tt.Logf("[1] evt=%s", evt)
			}),
			WithOnErrorFunc(func(err error) {
				tt.Fatalf("[1] on error=%+v", err)
				cancel()
			}),
		)
		if err != nil {
			tt.Fatalf("CreateVoter: %+v", err)
		}

		b2, err := CreateVoter(ctx, conf2,
			WithElectionTimeout(1*time.Second),
			WithObserveFunc(func(b *Bully, evt NodeEvent) {
				tt.Logf("[2] evt=%s", evt)
			}),
			WithOnErrorFunc(func(err error) {
				tt.Fatalf("[2] on error=%+v", err)
				cancel()
			}),
		)
		if err != nil {
			tt.Fatalf("CreateVoter: %+v", err)
		}

		tt.Logf("[1] addr=%v", b1.Address())
		tt.Logf("[2] addr=%v", b2.Address())

		if b1.IsVoter() != true {
			tt.Errorf("must voter node")
		}
		if b2.IsVoter() != true {
			tt.Errorf("must voter node")
		}

		if b1.IsLeader() != true {
			tt.Errorf("not join = is leader")
		}
		if b2.IsLeader() != true {
			tt.Errorf("not join = is leader")
		}

		if err := b1.Join(b1.Address()); err != nil {
			tt.Fatalf("join self: %+v", err)
		}
		tt.Logf("b1 join b1: %s", b1.Address())

		if err := b2.Join(b1.Address()); err != nil {
			tt.Fatalf("join b1: %+v", err)
		}
		tt.Logf("b2 join b1: %s", b1.Address())

		if b1.IsLeader() != true {
			tt.Errorf("leader = b1: %+v", b1.node)
		}

		if b2.IsLeader() {
			tt.Errorf("leader = b1: %+v", b2.node)
		}

		if err := b1.Leave(); err != nil {
			tt.Fatalf("Leave: %+v", err)
		}
		tt.Logf("b1 leave: %+v", b1.node)

		if b2.IsLeader() != true {
			tt.Errorf("leader = b2: %+v", b2.node)
		}
		tt.Logf("b2 is leader = %v", b2.IsLeader())

		if err := b2.Leave(); err != nil {
			tt.Fatalf("Leave: %+v", err)
		}
		tt.Logf("b2 leave: %+v", b2.node)
	})
}

func TestCreateNonVoter(t *testing.T) {
	t.Run("1voter+1nonvoter", func(tt *testing.T) {
		conf1 := memberlist.DefaultLocalConfig()
		conf1.Name = "test1"
		conf1.BindAddr = "127.0.0.1"
		conf1.BindPort = 0
		conf1.AdvertiseAddr = "127.0.0.1"
		conf1.AdvertisePort = 0
		conf1.EnableCompression = false

		conf2 := memberlist.DefaultLocalConfig()
		conf2.Name = "test2"
		conf2.BindAddr = "127.0.0.1"
		conf2.BindPort = 0
		conf2.AdvertiseAddr = "127.0.0.1"
		conf2.AdvertisePort = 0
		conf2.EnableCompression = false

		ctx, cancel := context.WithCancel(context.Background())
		b1, err := CreateVoter(ctx, conf1,
			WithElectionTimeout(1*time.Second),
			WithObserveFunc(func(b *Bully, evt NodeEvent) {
				tt.Logf("[1] evt=%s", evt)
			}),
			WithOnErrorFunc(func(err error) {
				tt.Fatalf("[1] on error=%+v", err)
				cancel()
			}),
		)
		if err != nil {
			tt.Fatalf("CreateVoter: %+v", err)
		}

		b2, err := CreateNonVoter(ctx, conf2,
			WithElectionTimeout(1*time.Second),
			WithObserveFunc(func(b *Bully, evt NodeEvent) {
				tt.Logf("[2] evt=%s", evt)
			}),
			WithOnErrorFunc(func(err error) {
				tt.Fatalf("[2] on error=%+v", err)
				cancel()
			}),
		)
		if err != nil {
			tt.Fatalf("CreateNonVoter: %+v", err)
		}

		if err := b2.Join(b1.Address()); err != nil {
			tt.Errorf("Join: %+v", err)
		}

		members := b2.Members()
		if len(members) != 2 {
			tt.Errorf("1 voter + 1 nonvoter")
		}
		numLeader := 0
		for _, m := range members {
			if m.IsLeader() {
				numLeader += 1
			}
		}
		if numLeader != 1 {
			tt.Errorf("one leader")
		}
	})
	t.Run("2voter+1nonvoter", func(tt *testing.T) {
		conf1 := memberlist.DefaultLocalConfig()
		conf1.Name = "test1"
		conf1.BindAddr = "127.0.0.1"
		conf1.BindPort = 0
		conf1.AdvertiseAddr = "127.0.0.1"
		conf1.AdvertisePort = 0
		conf1.EnableCompression = false

		conf2 := memberlist.DefaultLocalConfig()
		conf2.Name = "test2"
		conf2.BindAddr = "127.0.0.1"
		conf2.BindPort = 0
		conf2.AdvertiseAddr = "127.0.0.1"
		conf2.AdvertisePort = 0
		conf2.EnableCompression = false

		conf3 := memberlist.DefaultLocalConfig()
		conf3.Name = "test3"
		conf3.BindAddr = "127.0.0.1"
		conf3.BindPort = 0
		conf3.AdvertiseAddr = "127.0.0.1"
		conf3.AdvertisePort = 0
		conf3.EnableCompression = false

		ctx, cancel := context.WithCancel(context.Background())
		b1, err := CreateVoter(ctx, conf1,
			WithElectionTimeout(1*time.Second),
			WithObserveFunc(func(b *Bully, evt NodeEvent) {
				tt.Logf("[1] evt=%s", evt)
			}),
			WithOnErrorFunc(func(err error) {
				tt.Fatalf("[1] on error=%+v", err)
				cancel()
			}),
		)
		if err != nil {
			tt.Fatalf("CreateVoter: %+v", err)
		}

		b2, err := CreateVoter(ctx, conf2,
			WithElectionTimeout(1*time.Second),
			WithObserveFunc(func(b *Bully, evt NodeEvent) {
				tt.Logf("[2] evt=%s", evt)
			}),
			WithOnErrorFunc(func(err error) {
				tt.Fatalf("[2] on error=%+v", err)
				cancel()
			}),
		)
		if err != nil {
			tt.Fatalf("CreateVoter: %+v", err)
		}

		b3, err := CreateNonVoter(ctx, conf3,
			WithElectionTimeout(1*time.Second),
			WithObserveFunc(func(b *Bully, evt NodeEvent) {
				tt.Logf("[3] evt=%s", evt)
			}),
			WithOnErrorFunc(func(err error) {
				tt.Fatalf("[3] on error=%+v", err)
				cancel()
			}),
		)
		if err != nil {
			tt.Fatalf("CreateNonVoter: %+v", err)
		}

		if err := b2.Join(b1.Address()); err != nil {
			tt.Errorf("Join: %+v", err)
		}
		if err := b3.Join(b1.Address()); err != nil {
			tt.Errorf("Join: %+v", err)
		}

		members := b3.Members()
		if len(members) != 3 {
			tt.Errorf("2 voter + 1 nonvoter")
		}
		numLeader := 0
		for _, m := range members {
			if m.IsLeader() {
				numLeader += 1
			}
		}
		if numLeader != 1 {
			tt.Errorf("one leader")
		}
	})

	t.Run("LeadershipTransfer", func(tt *testing.T) {
		conf1 := memberlist.DefaultLocalConfig()
		conf1.Name = "test1"
		conf1.BindAddr = "127.0.0.1"
		conf1.BindPort = 0
		conf1.AdvertiseAddr = "127.0.0.1"
		conf1.AdvertisePort = 0
		conf1.EnableCompression = false

		conf2 := memberlist.DefaultLocalConfig()
		conf2.Name = "test2"
		conf2.BindAddr = "127.0.0.1"
		conf2.BindPort = 0
		conf2.AdvertiseAddr = "127.0.0.1"
		conf2.AdvertisePort = 0
		conf2.EnableCompression = false

		conf3 := memberlist.DefaultLocalConfig()
		conf3.Name = "test3"
		conf3.BindAddr = "127.0.0.1"
		conf3.BindPort = 0
		conf3.AdvertiseAddr = "127.0.0.1"
		conf3.AdvertisePort = 0
		conf3.EnableCompression = false

		ctx, cancel := context.WithCancel(context.Background())
		b1, err := CreateVoter(ctx, conf1,
			WithObserveFunc(func(b *Bully, evt NodeEvent) {
				tt.Logf("[1] evt=%s", evt)
			}),
			WithOnErrorFunc(func(err error) {
				tt.Fatalf("[1] on error=%+v", err)
				cancel()
			}),
		)
		if err != nil {
			tt.Fatalf("CreateVoter: %+v", err)
		}
		tt.Logf("[1] addr=%s", b1.Address())

		b2, err := CreateVoter(ctx, conf2,
			WithObserveFunc(func(b *Bully, evt NodeEvent) {
				tt.Logf("[2] evt=%s", evt)
			}),
			WithOnErrorFunc(func(err error) {
				tt.Fatalf("[2] on error=%+v", err)
				cancel()
			}),
		)
		if err != nil {
			tt.Fatalf("CreateVoter: %+v", err)
		}
		tt.Logf("[2] addr=%s", b2.Address())

		b3, err := CreateNonVoter(ctx, conf3,
			WithObserveFunc(func(b *Bully, evt NodeEvent) {
				tt.Logf("[3] evt=%s", evt)
			}),
			WithOnErrorFunc(func(err error) {
				tt.Fatalf("[3] on error=%+v", err)
				cancel()
			}),
		)
		if err != nil {
			tt.Fatalf("CreateNonVoter: %+v", err)
		}
		tt.Logf("[3] addr=%s", b3.Address())

		if err := b2.Join(b1.Address()); err != nil {
			tt.Fatalf("Join: %+v", err)
		}
		if err := b3.Join(b1.Address()); err != nil {
			tt.Fatalf("Join: %+v", err)
		}

		members := b3.Members()
		if len(members) != 3 {
			tt.Errorf("2 voter + 1 nonvoter")
		}
		leaderID := ""
		numLeader := 0
		for _, m := range members {
			if m.IsLeader() {
				numLeader += 1
				leaderID = m.ID()
			}
		}
		if numLeader != 1 {
			tt.Errorf("one leader")
		}
		if leaderID != b1.ID() {
			tt.Errorf("leader = b1: %s", leaderID)
		}

		c, can := context.WithTimeout(ctx, 10*time.Second)
		defer can()
		if err := b1.LeadershipTransfer(c); err != nil {
			tt.Fatalf("LeadershipTransfer: %+v", err)
		}

		members = b3.Members()
		if len(members) != 3 {
			tt.Errorf("2 voter + 1 nonvoter")
		}
		leaderID = ""
		numLeader = 0
		for _, m := range members {
			if m.IsLeader() {
				numLeader += 1
				leaderID = m.ID()
			}
		}
		if numLeader != 1 {
			tt.Errorf("one leader")
		}
		if leaderID != b2.ID() {
			tt.Errorf("leader = b2: %s", leaderID)
		}
	})
}

func TestMetadata(t *testing.T) {
	conf1 := memberlist.DefaultLocalConfig()
	conf1.Name = "test1"
	conf1.BindAddr = "127.0.0.1"
	conf1.BindPort = 0
	conf1.AdvertiseAddr = "127.0.0.1"
	conf1.AdvertisePort = 0
	conf1.EnableCompression = false

	conf2 := memberlist.DefaultLocalConfig()
	conf2.Name = "test2"
	conf2.BindAddr = "127.0.0.1"
	conf2.BindPort = 0
	conf2.AdvertiseAddr = "127.0.0.1"
	conf2.AdvertisePort = 0
	conf2.EnableCompression = false

	ctx, cancel := context.WithCancel(context.Background())
	b1, err := CreateVoter(ctx, conf1,
		WithElectionTimeout(1*time.Second),
		WithObserveFunc(func(b *Bully, evt NodeEvent) {
			t.Logf("[1] evt=%s", evt)
		}),
		WithOnErrorFunc(func(err error) {
			t.Fatalf("[1] on error=%+v", err)
			cancel()
		}),
	)
	if err != nil {
		t.Fatalf("CreateVoter: %+v", err)
	}

	b2, err := CreateVoter(ctx, conf2,
		WithElectionTimeout(1*time.Second),
		WithObserveFunc(func(b *Bully, evt NodeEvent) {
			t.Logf("[2] evt=%s", evt)
		}),
		WithOnErrorFunc(func(err error) {
			t.Fatalf("[2] on error=%+v", err)
			cancel()
		}),
	)
	if err != nil {
		t.Fatalf("CreateVoter: %+v", err)
	}

	if err := b2.Join(b1.Address()); err != nil {
		t.Errorf("Join: %+v", err)
	}

	if err := b1.UpdateMetadata([]byte("hello")); err != nil {
		t.Errorf("UpdateMetadata")
	}

	//t.Logf("TODO detect update")
	time.Sleep(100 * time.Millisecond)

	for _, m := range b2.Members() {
		if m.ID() == "test1" {
			data := m.UserMetadata()
			if bytes.Equal(data, []byte("hello")) != true {
				t.Errorf("first update")
			}
		}
	}

	if err := b1.UpdateMetadata([]byte("world")); err != nil {
		t.Errorf("UpdateMetadata")
	}

	//t.Logf("TODO detect update")
	time.Sleep(100 * time.Millisecond)

	for _, m := range b2.Members() {
		if m.ID() == "test1" {
			data := m.UserMetadata()
			if bytes.Equal(data, []byte("world")) != true {
				t.Errorf("first update")
			}
		}
	}
}

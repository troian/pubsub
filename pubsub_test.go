// Copyright 2013, Chandra Sekar S.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the README.md file.

package pubsub

import (
	"context"
	"testing"

	check "gopkg.in/check.v1"
)

var _ = check.Suite(new(Suite))

func Test(t *testing.T) {
	// start := runtime.NumGoroutine()

	check.TestingT(t)

	// time.Sleep(1000 * time.Millisecond)
	// end := runtime.NumGoroutine()
	// t.Log("# GoRoutines start:", start, " end:", end)
	// if end != start {
	// 	t.Fail()
	// }
}

type Suite struct{}

func (s *Suite) TestSub(c *check.C) {
	ps := New(context.Background(), 1)
	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t1")
	ch3 := ps.Sub("t2")

	ps.Pub("hi", []string{"t1"})
	c.Check(<-ch1, check.Equals, "hi")
	c.Check(<-ch2, check.Equals, "hi")

	ps.Pub("hello", []string{"t2"})
	c.Check(<-ch3, check.Equals, "hello")

	ps.Shutdown()
	_, ok := <-ch1
	c.Check(ok, check.Equals, false)
	_, ok = <-ch2
	c.Check(ok, check.Equals, false)
	_, ok = <-ch3
	c.Check(ok, check.Equals, false)
}

func (s *Suite) TestRetained(c *check.C) {
	ps := New(context.Background(), 1)
	ch1 := ps.Sub("t1")

	ps.Pub("hi", []string{"t1"}, WithRetain())
	c.Check(<-ch1, check.Equals, "hi")

	ch2 := ps.Sub("t1")
	c.Check(<-ch2, check.Equals, "hi")

	ps.Shutdown()
	_, ok := <-ch1
	c.Check(ok, check.Equals, false)
	_, ok = <-ch2
	c.Check(ok, check.Equals, false)
}

func (s *Suite) TestSubUnbuffered(c *check.C) {
	ps := New(context.Background(), 1)

	ch1 := ps.Sub("t1")
	ch3 := ps.Sub("t2")

	go func() { ps.Pub("hi", []string{"t1"}) }()
	c.Check(<-ch1, check.Equals, "hi")

	go func() { ps.Pub("hello", []string{"t2"}) }()
	c.Check(<-ch3, check.Equals, "hello")

	ps.Shutdown()
	_, ok := <-ch1
	c.Check(ok, check.Equals, false)
	_, ok = <-ch3
	c.Check(ok, check.Equals, false)
}

func (s *Suite) TestSubOnce(c *check.C) {
	ps := New(context.Background(), 1)
	ch := ps.SubOnce("t1")

	ps.Pub("hi", []string{"t1"})
	c.Check(<-ch, check.Equals, "hi")

	val, ok := <-ch
	c.Check(val, check.Equals, nil)
	c.Check(ok, check.Equals, false)
	ps.Shutdown()
}

func (s *Suite) TestAddSub(c *check.C) {
	ps := New(context.Background(), 1)
	ch1 := ps.Sub("t1")

	ch2 := ps.Sub("t2")

	ps.Pub("hi1", []string{"t1"})
	c.Check(<-ch1, check.Equals, "hi1")

	ps.Pub("hi2", []string{"t2"})
	c.Check(<-ch2, check.Equals, "hi2")

	ps.AddTopics(ch1, "t2", "t3")
	ps.Pub("hi3", []string{"t2"})
	c.Check(<-ch1, check.Equals, "hi3")
	c.Check(<-ch2, check.Equals, "hi3")

	ps.Pub("hi4", []string{"t3"})
	c.Check(<-ch1, check.Equals, "hi4")

	ps.Shutdown()
}

func (s *Suite) TestUnsub(c *check.C) {
	ps := New(context.Background(), 1)
	ch := ps.Sub("t1")

	ps.Pub("hi", []string{"t1"})
	c.Check(<-ch, check.Equals, "hi")

	ps.Unsub(ch, "t1")
	_, ok := <-ch
	c.Check(ok, check.Equals, false)
	ps.Shutdown()
}

func (s *Suite) TestUnsubAll(c *check.C) {
	ps := New(context.Background(), 1)
	ch1 := ps.Sub("t1", "t2", "t3")
	ch2 := ps.Sub("t1", "t3")

	c.Log("waiting for unsub")
	ps.Unsub(ch1)

	c.Log("waiting for close")
	m, ok := <-ch1
	c.Check(ok, check.Equals, false)

	ps.Pub("hi", []string{"t1"})
	c.Log("waiting for ch2")
	m, ok = <-ch2
	c.Check(m, check.Equals, "hi")

	c.Log("waiting for shutdown")
	ps.Shutdown()
}

func (s *Suite) TestDrainOnUnsub(c *check.C) {
	ps := New(context.Background(), 1)

	ch1 := ps.Sub("t1", "t2", "t3")

	ps.Pub("test", []string{"t1", "t2", "t3"})
	ps.Unsub(ch1)

	_, ok := <-ch1
	c.Check(ok, check.Equals, true)

	ps.Shutdown()
}

func (s *Suite) TestMultiSub(c *check.C) {
	ps := New(context.Background(), 1)
	ch := ps.Sub("t1", "t2")

	ps.Pub("hi", []string{"t1"})
	c.Check(<-ch, check.Equals, "hi")

	ps.Pub("hello", []string{"t2"})
	c.Check(<-ch, check.Equals, "hello")

	ps.Shutdown()
	_, ok := <-ch
	c.Check(ok, check.Equals, false)
}

func (s *Suite) TestMultiSubOnce(c *check.C) {
	ps := New(context.Background(), 1)
	ch := ps.SubOnce("t1", "t2")

	ps.Pub("hi", []string{"t1"})
	c.Check(<-ch, check.Equals, "hi")

	ps.Pub("hello", []string{"t2"})

	_, ok := <-ch
	c.Check(ok, check.Equals, false)

	ps.Shutdown()
}

func (s *Suite) TestMultiPub(c *check.C) {
	ps := New(context.Background(), 1)
	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t2")

	ps.Pub("hi", []string{"t1", "t2"})
	c.Check(<-ch1, check.Equals, "hi")
	c.Check(<-ch2, check.Equals, "hi")

	ps.Shutdown()
}

func (s *Suite) TestMultiUnsub(c *check.C) {
	ps := New(context.Background(), 1)
	ch := ps.Sub("t1", "t2", "t3")

	ps.Unsub(ch, "t1")

	ps.Pub("hi", []string{"t1"})

	ps.Pub("hello", []string{"t2"})
	c.Check(<-ch, check.Equals, "hello")

	ps.Unsub(ch, "t2", "t3")
	_, ok := <-ch
	c.Check(ok, check.Equals, false)

	ps.Shutdown()
}

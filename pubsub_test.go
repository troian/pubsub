// Copyright 2013, Chandra Sekar S.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the README.md file.

package pubsub

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type testSuite struct {
	suite.Suite
	ctx context.Context
}

func (s *testSuite) waitForMessage(ch <-chan interface{}) string {
	s.T().Helper()

	tctx, tcancel := context.WithTimeout(s.ctx, 3*time.Second)
	defer tcancel()

	select {
	case <-s.ctx.Done():
		tcancel()
		s.T().FailNow()
	case <-tctx.Done():
		tcancel()
		s.T().FailNow()
	case m, ok := <-ch:
		tcancel()
		if !ok {
			s.T().FailNow()
		}

		require.IsType(s.T(), *new(string), m)
		return m.(string)
	}

	return ""
}

func TestIntegrationTestSuite(t *testing.T) {
	start := runtime.NumGoroutine()

	ctx := context.Background()
	ts := &testSuite{
		ctx: ctx,
	}

	suite.Run(t, ts)

	end := runtime.NumGoroutine()

	if end != start {
		t.Log("# GoRoutines start:", start, " end:", end)
		t.Fail()
	}
}

func (s *testSuite) SetupSuite() {}

func (s *testSuite) TearDownSuite() {}

func (s *testSuite) TestSub() {
	ps := New(s.ctx, 1)

	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t1")
	ch3 := ps.Sub("t2")

	ps.Pub("hi", []string{"t1"})

	msg := s.waitForMessage(ch1)
	require.Equal(s.T(), "hi", msg)

	msg = s.waitForMessage(ch2)
	require.Equal(s.T(), "hi", msg)

	ps.Pub("hello", []string{"t2"})

	msg = s.waitForMessage(ch3)
	require.Equal(s.T(), "hello", msg)

	ps.Shutdown()
	_, open := <-ch1
	assert.False(s.T(), open)
	_, open = <-ch2
	assert.False(s.T(), open)
	_, open = <-ch3
	assert.False(s.T(), open)
}

func (s *testSuite) TestRetained() {
	ps := New(s.ctx, 1)
	ch1 := ps.Sub("t1")

	ps.Pub("hi", []string{"t1"}, WithRetain())

	msg := s.waitForMessage(ch1)
	require.Equal(s.T(), "hi", msg)

	ch2 := ps.Sub("t1")
	msg = s.waitForMessage(ch2)
	require.Equal(s.T(), "hi", msg)

	ps.Shutdown()

	_, open := <-ch1
	assert.False(s.T(), open)
	_, open = <-ch2
	assert.False(s.T(), open)
}

func (s *testSuite) TestRetained2() {
	ps := New(s.ctx, 1)

	ps.Pub("hi", []string{"t1"}, WithRetain())

	ch1 := ps.Sub("t1")
	msg := s.waitForMessage(ch1)
	require.Equal(s.T(), "hi", msg)

	ps.Shutdown()
	_, open := <-ch1
	assert.False(s.T(), open)
}

func (s *testSuite) TestSubUnbuffered() {
	ps := New(s.ctx, 1)

	ch1 := ps.Sub("t1")
	ch3 := ps.Sub("t2")

	go func() { ps.Pub("hi", []string{"t1"}) }()

	msg := s.waitForMessage(ch1)
	require.Equal(s.T(), "hi", msg)

	go func() { ps.Pub("hello", []string{"t2"}) }()

	msg = s.waitForMessage(ch3)
	require.Equal(s.T(), "hello", msg)

	ps.Shutdown()

	_, open := <-ch1
	assert.False(s.T(), open)
	_, open = <-ch3
	assert.False(s.T(), open)
}

func (s *testSuite) TestSubOnce() {
	ps := New(s.ctx, 1)
	ch := ps.SubOnce("t1")

	ps.Pub("hi", []string{"t1"})

	msg := s.waitForMessage(ch)
	require.Equal(s.T(), "hi", msg)

	val, open := <-ch
	assert.Nil(s.T(), val)
	assert.False(s.T(), open)

	ps.Shutdown()
}

func (s *testSuite) TestAddSub() {
	ps := New(s.ctx, 1)
	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t2")

	ps.Pub("hi1", []string{"t1"})

	msg := s.waitForMessage(ch1)
	require.Equal(s.T(), "hi1", msg)

	ps.Pub("hi2", []string{"t2"})

	msg = s.waitForMessage(ch2)
	require.Equal(s.T(), "hi2", msg)

	ps.AddTopics(ch1, "t2", "t3")

	ps.Pub("hi3", []string{"t2"})

	msg = s.waitForMessage(ch1)
	require.Equal(s.T(), "hi3", msg)

	msg = s.waitForMessage(ch2)
	require.Equal(s.T(), "hi3", msg)

	ps.Pub("hi4", []string{"t3"})

	msg = s.waitForMessage(ch1)
	require.Equal(s.T(), "hi4", msg)

	ps.Shutdown()

	val, open := <-ch2
	assert.Nil(s.T(), val)
	assert.False(s.T(), open)
}

func (s *testSuite) TestUnsub() {
	ps := New(s.ctx, 1)
	ch := ps.Sub("t1")

	ps.Pub("hi", []string{"t1"})

	msg := s.waitForMessage(ch)
	require.Equal(s.T(), "hi", msg)

	ps.Unsub(ch, "t1")

	_, open := <-ch
	assert.False(s.T(), open)

	ps.Shutdown()
}

func (s *testSuite) TestUnsubAll() {
	ps := New(s.ctx, 1)
	ch1 := ps.Sub("t1", "t2", "t3")
	ch2 := ps.Sub("t1", "t3")

	ps.Unsub(ch1)

	_, open := <-ch1
	assert.False(s.T(), open)

	ps.Pub("hi", []string{"t1"})

	msg := s.waitForMessage(ch2)
	require.Equal(s.T(), "hi", msg)

	ps.Shutdown()
}

func (s *testSuite) TestDrainOnUnsub() {
	ps := New(s.ctx, 1)

	ch1 := ps.Sub("t1", "t2", "t3")

	ps.Pub("test", []string{"t1", "t2", "t3"})

	ps.Unsub(ch1)

	received := 0

	// should receive at most 3 messages
	for {
		msg, open := <-ch1
		if msg != nil {
			received++
		} else {
			assert.False(s.T(), open)
			break
		}

		if received > 3 {
			break
		}
	}

	require.LessOrEqual(s.T(), received, 3)

	_, open := <-ch1
	assert.False(s.T(), open)

	ps.Shutdown()
}

func (s *testSuite) TestMultiSub() {
	ps := New(s.ctx, 1)
	ch := ps.Sub("t1", "t2")

	ps.Pub("hi", []string{"t1"})

	msg := s.waitForMessage(ch)
	require.Equal(s.T(), "hi", msg)

	ps.Pub("hello", []string{"t2"})

	msg = s.waitForMessage(ch)
	require.Equal(s.T(), "hello", msg)

	ps.Shutdown()

	_, open := <-ch
	assert.False(s.T(), open)
}

func (s *testSuite) TestMultiSubOnce() {
	ps := New(s.ctx, 1)
	ch := ps.SubOnce("t1", "t2")

	ps.Pub("hi", []string{"t1"})

	msg := s.waitForMessage(ch)
	require.Equal(s.T(), "hi", msg)

	ps.Pub("hello", []string{"t2"})

	val, open := <-ch
	assert.False(s.T(), open)
	assert.Nil(s.T(), val)

	ps.Shutdown()
}

func (s *testSuite) TestMultiPub() {
	ps := New(s.ctx, 1)
	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t2")

	ps.Pub("hi", []string{"t1", "t2"})

	msg := s.waitForMessage(ch1)
	require.Equal(s.T(), "hi", msg)

	msg = s.waitForMessage(ch2)
	require.Equal(s.T(), "hi", msg)

	ps.Shutdown()
}

func (s *testSuite) TestMultiUnsub() {
	ps := New(s.ctx, 1)
	ch := ps.Sub("t1", "t2", "t3")

	ps.Unsub(ch, "t1")

	ps.Pub("hi", []string{"t1"})
	ps.Pub("hello", []string{"t2"})

	msg := s.waitForMessage(ch)
	require.Equal(s.T(), "hello", msg)

	ps.Unsub(ch, "t2", "t3")

	val, open := <-ch
	assert.False(s.T(), open)
	assert.Nil(s.T(), val)

	ps.Shutdown()
}

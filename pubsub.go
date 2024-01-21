// Copyright 2013, Chandra Sekar S.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the README.md file.

// Package pubsub implements a simple multi-topic pub-sub
// library.
//
// Topics must be strings and messages of any type can be
// published. A topic can have any number of subscribers and
// all of them receive messages published on the topic.
package pubsub

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
)

type operation int

var ErrorCongestionMsgLost = errors.New("PubSub Congestion: message lost, try again")

const (
	sub operation = iota
	subOnce
	subAdd
	unsub
)

type pubOptions struct {
	retain bool
}

type PubOption func(*pubOptions)

func WithRetain() PubOption {
	return func(options *pubOptions) {
		options.retain = true
	}
}

type Publisher interface {
	Pub(msg interface{}, topics []string, opts ...PubOption)
	TryPub(msg interface{}, topics []string, opts ...PubOption) bool
}

type Subscriber interface {
	Sub(topics ...string) <-chan interface{}
	SubOnce(topics ...string) <-chan interface{}
	AddTopics(ch <-chan interface{}, topics ...string)
	Unsub(ch <-chan interface{}, topics ...string)
}

type PubSub interface {
	Publisher
	Subscriber
	Shutdown()
}

// pubSub is a collection of topics.
type pubSub struct {
	ctx       context.Context
	cancel    context.CancelFunc
	group     *errgroup.Group
	cmdch     chan cmd
	msgch     chan msgEnvelope
	topics    topics
	revTopics revTopics
	capacity  int
}

type topicSubs map[<-chan interface{}]*subscriber

type topic struct {
	retained interface{}
	subs     topicSubs
}

type revSubscriber struct {
	sub    *subscriber
	topics map[string]bool
}

type topics map[string]*topic
type revTopics map[<-chan interface{}]*revSubscriber

type cmd struct {
	op     operation
	topics []string
	ch     interface{}
	done   chan<- bool
}

type msgEnvelope struct {
	topics []string
	ch     chan interface{}
	msg    interface{}
	opts   pubOptions
}

type subscriber struct {
	ctx   context.Context
	och   chan interface{}
	ich   chan interface{}
	done  chan struct{}
	cmdch chan<- cmd
	once  bool
}

// New creates a new PubSub and starts a goroutine for handling operations.
// The capacity of the channels created by Sub and SubOnce will be as specified.
// The default is the blocking variant, since this is the original behaviour
func New(ctx context.Context, capacity int) PubSub {
	ctx, cancel := context.WithCancel(ctx)
	group, ctx := errgroup.WithContext(ctx)

	ps := &pubSub{
		ctx:       ctx,
		cancel:    cancel,
		group:     group,
		cmdch:     make(chan cmd),
		msgch:     make(chan msgEnvelope, capacity),
		topics:    make(topics),
		revTopics: make(revTopics),
		capacity:  capacity,
	}

	ps.group.Go(func() error {
		return ps.start()
	})

	return ps
}

// Sub returns a channel on which messages published on any of
// the specified topics can be received.
func (ps *pubSub) Sub(topics ...string) <-chan interface{} {
	return ps.sub(sub, topics...)
}

// SubOnce is similar to Sub, but only the first message published, after subscription,
// on any of the specified topics can be received.
// Note that if you DO NOT CLEAR the channel in time, ALL PUBSUBs will be blocked
func (ps *pubSub) SubOnce(topics ...string) <-chan interface{} {
	return ps.sub(subOnce, topics...)
}

func (ps *pubSub) sub(op operation, topics ...string) <-chan interface{} {
	done := make(chan bool, 1)
	ch := make(chan interface{}, ps.capacity)

	select {
	case <-ps.ctx.Done():
	case ps.cmdch <- cmd{op: op, topics: topics, ch: ch, done: done}:
	}

	select {
	case <-ps.ctx.Done():
	case <-done:
	}

	return ch
}

// AddTopics adds subscriptions to an existing channel.
func (ps *pubSub) AddTopics(ch <-chan interface{}, topics ...string) {
	done := make(chan bool, 1)

	select {
	case <-ps.ctx.Done():
	case ps.cmdch <- cmd{op: subAdd, topics: topics, ch: ch, done: done}:
	}

	select {
	case <-ps.ctx.Done():
	case <-done:
	}
}

// Pub publishes the given message to all subscribers of the specified topics.
func (ps *pubSub) Pub(msg interface{}, topics []string, opts ...PubOption) {
	op := pubOptions{}

	for _, opt := range opts {
		opt(&op)
	}

	select {
	case <-ps.ctx.Done():
	case ps.msgch <- msgEnvelope{topics: topics, msg: msg, opts: op}:
	}
}

// TryPub publishes the given message with Non-Blocking semantics
// Use this only for messages that are ok to lose
// Use this only if your calling function must never ever block.
// returns an error on congestion of cmd-pipe, in which case you may try again,
// since the cmd-pipe is filled with sub-requests and other threads messages
// and can easily become full.
// Note that err=nil does not mean the message arrived, it could still
// get lost at the receivers end if that channel is full
// If err!=nil, one idea could be to spawn an anonymous goroutine to try again
func (ps *pubSub) TryPub(msg interface{}, topics []string, opts ...PubOption) bool {
	op := pubOptions{}

	for _, opt := range opts {
		opt(&op)
	}

	select {
	case <-ps.ctx.Done():
		return false
	case ps.msgch <- msgEnvelope{topics: topics, msg: msg, opts: op}:
	default:
		return false
	}
	return true
}

// Unsub unsubscribes the given channel from the specified
// topics. If no topic is specified, it is unsubscribed
// from all topics.
func (ps *pubSub) Unsub(ch <-chan interface{}, topics ...string) {
	done := make(chan bool, 1)

	c := cmd{
		op:     unsub,
		topics: topics,
		ch:     ch,
		done:   done,
	}

	select {
	case <-ps.ctx.Done():
	case ps.cmdch <- c:
	}

	select {
	case <-ps.ctx.Done():
	case <-done:
	}
}

// Shutdown closes all subscribed channels and terminates the goroutine.
func (ps *pubSub) Shutdown() {
	ps.cancel()

	_ = ps.group.Wait()
}

// Main Cmd Handling Goroutine
func (ps *pubSub) start() error {
	for {
		select {
		case <-ps.ctx.Done():
			return ps.ctx.Err()
		case cmd := <-ps.cmdch:
			switch cmd.op {
			case sub:
				fallthrough
			case subOnce:
				subs := &subscriber{
					ctx:   ps.ctx,
					och:   cmd.ch.(chan interface{}),
					ich:   make(chan interface{}, 1),
					done:  make(chan struct{}),
					cmdch: ps.cmdch,
					once:  cmd.op == subOnce,
				}
				ps.add(cmd.topics, subs)
				ps.group.Go(subs.run)
			case subAdd:
				ch := cmd.ch.(<-chan interface{})
				subs := ps.revTopics[ch].sub

				ps.add(cmd.topics, subs)
			case unsub:
				ps.remove(cmd.topics, cmd.ch.(<-chan interface{}))
			}

			select {
			case <-ps.ctx.Done():
				return ps.ctx.Err()
			case cmd.done <- true:
			default:
			}
		case msg := <-ps.msgch:
			for _, topic := range msg.topics {
				ps.send(topic, msg.msg, msg.opts)
			}
		}
	}
}

func (s *subscriber) run() error {
	defer func() {
		close(s.och)
	}()

	var pending []interface{}
	var msg interface{}
	var och chan interface{}
	var cmdch chan<- cmd

	done := make(chan bool, 1)

	c := cmd{op: unsub, ch: (<-chan interface{})(s.och), done: done}

	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-s.done:
			return nil
		case <-done:
			return nil
		case rMsg := <-s.ich:
			if cmdch == nil {
				pending = append(pending, rMsg)
				if och == nil {
					msg = pending[0]
					och = s.och
				}
			}
		case och <- msg:
			if s.once {
				cmdch = s.cmdch
				och = nil
				break
			}

			pending = pending[1:]
			if len(pending) > 0 {
				msg = pending[0]
			} else {
				och = nil
			}
		case cmdch <- c:
		}
	}
}

func (ps *pubSub) add(topics []string, sub *subscriber) {
	for _, tn := range topics {
		tp := ps.topics[tn]
		if tp == nil {
			tp = &topic{}
			ps.topics[tn] = tp
		}
		if tp.subs == nil {
			tp.subs = make(topicSubs)
		}

		tp.subs[sub.och] = sub

		if ps.revTopics[sub.och] == nil {
			ps.revTopics[sub.och] = &revSubscriber{
				sub:    sub,
				topics: make(map[string]bool),
			}
		}

		ps.revTopics[sub.och].topics[tn] = true

		if tp.retained != nil {
			sub.och <- ps.topics[tn].retained
		}
	}
}

func (ps *pubSub) send(topic string, msg interface{}, opts pubOptions) {
	topicParams := ps.topics[topic]

	if topicParams == nil {
		return
	}

	if opts.retain {
		topicParams.retained = msg
	}

	for _, sub := range topicParams.subs {
		select {
		case <-ps.ctx.Done():
			return
		case sub.ich <- msg:
		}
	}
}

func (ps *pubSub) remove(topics []string, ch <-chan interface{}) {
	subscribedTopics := ps.revTopics[ch].topics

	subs := ps.revTopics[ch].sub

	if len(topics) == 0 {
		topics = make([]string, 0, len(subscribedTopics))

		for topic := range subscribedTopics {
			topics = append(topics, topic)
		}
	}

	for _, topic := range topics {
		delete(ps.topics[topic].subs, ch)
		delete(ps.revTopics[ch].topics, topic)
	}

	if len(ps.revTopics[ch].topics) == 0 {
		ps.revTopics[ch].sub = nil

		close(subs.done)
		delete(ps.revTopics, ch)
	}
}

package neko_go_db

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrNekoGoDBNil = errors.New("neko_go_db: nil")

type nekoRedisOp int

const (
	nekoRedisSet nekoRedisOp = iota + 1
	nekoRedisGet
	nekoRedisDel
	nekoRedisHSet
	nekoRedisHGet
	nekoRedisHDel
	nekoRedisZAdd
	nekoRedisZRange
	nekoRedisZCard
	nekoRedisZRem
)

type nekoRedisStringValue struct {
	Value     string
	ExpiresAt time.Time
}

type ZMember struct {
	Member string
	Score  float64
}

type nekoRedisRequest struct {
	Op     nekoRedisOp
	Key    string
	Value  string
	TTL    time.Duration
	NX     bool
	Field  string
	Fields []string
	ZPairs []ZMember
	Start  int
	Stop   int
	Reply  chan nekoRedisResponse
}

type nekoRedisResponse struct {
	Value  string
	Values []string
	Int    int64
	OK     bool
	IsNil  bool
}

type nekoRedisShard struct {
	ch chan nekoRedisRequest
}

type NekoGoDB struct {
	shards []nekoRedisShard
	pubsub *nekoPubSubHub
}

type nekoPubSubHub struct {
	mu   sync.RWMutex
	subs map[string]map[*NekoGoDBSubscription]struct{}
}

type NekoGoDBSubscription struct {
	hub      *nekoPubSubHub
	channels []string
	messages chan nekoPubSubMessage
	closed   chan struct{}
	once     sync.Once
}

type nekoPubSubMessage struct {
	Channel string
	Payload string
}

type NekoGoDBServer struct {
	cfg   Config
	store *NekoGoDB
}

func NewNekoGoDB(cfg Config) *NekoGoDB {
	shards := cfg.RedisShards
	if shards < 1 {
		shards = 1
	}

	db := &NekoGoDB{
		shards: make([]nekoRedisShard, shards),
		pubsub: &nekoPubSubHub{subs: make(map[string]map[*NekoGoDBSubscription]struct{})},
	}

	for i := range db.shards {
		db.shards[i].ch = make(chan nekoRedisRequest, 4096)
		go db.runShard(db.shards[i].ch)
	}

	return db
}

func (db *NekoGoDB) Shards() int {
	if db == nil {
		return 0
	}
	return len(db.shards)
}

func (db *NekoGoDB) runShard(ch <-chan nekoRedisRequest) {
	stringsMap := make(map[string]nekoRedisStringValue)
	hashes := make(map[string]map[string]string)
	zsets := make(map[string]map[string]float64)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case req, ok := <-ch:
			if !ok {
				return
			}
			db.handleShardRequest(stringsMap, hashes, zsets, req)
		case <-ticker.C:
			now := time.Now().UTC()
			for key, value := range stringsMap {
				if !value.ExpiresAt.IsZero() && now.After(value.ExpiresAt) {
					delete(stringsMap, key)
				}
			}
		}
	}
}

func (db *NekoGoDB) handleShardRequest(stringsMap map[string]nekoRedisStringValue, hashes map[string]map[string]string, zsets map[string]map[string]float64, req nekoRedisRequest) {
	now := time.Now().UTC()
	reply := nekoRedisResponse{}

	switch req.Op {
	case nekoRedisSet:
		current, exists := stringsMap[req.Key]
		if exists && !current.ExpiresAt.IsZero() && now.After(current.ExpiresAt) {
			delete(stringsMap, req.Key)
			exists = false
		}
		if req.NX && exists {
			reply.OK = false
			break
		}
		value := nekoRedisStringValue{Value: req.Value}
		if req.TTL > 0 {
			value.ExpiresAt = now.Add(req.TTL)
		}
		stringsMap[req.Key] = value
		reply.OK = true
	case nekoRedisGet:
		value, exists := stringsMap[req.Key]
		if !exists {
			reply.IsNil = true
			break
		}
		if !value.ExpiresAt.IsZero() && now.After(value.ExpiresAt) {
			delete(stringsMap, req.Key)
			reply.IsNil = true
			break
		}
		reply.Value = value.Value
	case nekoRedisDel:
		var removed int64
		if _, ok := stringsMap[req.Key]; ok {
			delete(stringsMap, req.Key)
			removed++
		}
		if _, ok := hashes[req.Key]; ok {
			delete(hashes, req.Key)
			removed++
		}
		if _, ok := zsets[req.Key]; ok {
			delete(zsets, req.Key)
			removed++
		}
		reply.Int = removed
	case nekoRedisHSet:
		hash := hashes[req.Key]
		if hash == nil {
			hash = make(map[string]string)
			hashes[req.Key] = hash
		}
		_, existed := hash[req.Field]
		hash[req.Field] = req.Value
		if !existed {
			reply.Int = 1
		}
	case nekoRedisHGet:
		hash := hashes[req.Key]
		if hash == nil {
			reply.IsNil = true
			break
		}
		value, ok := hash[req.Field]
		if !ok {
			reply.IsNil = true
			break
		}
		reply.Value = value
	case nekoRedisHDel:
		hash := hashes[req.Key]
		if hash == nil {
			break
		}
		for _, field := range req.Fields {
			if _, ok := hash[field]; ok {
				delete(hash, field)
				reply.Int++
			}
		}
		if len(hash) == 0 {
			delete(hashes, req.Key)
		}
	case nekoRedisZAdd:
		set := zsets[req.Key]
		if set == nil {
			set = make(map[string]float64)
			zsets[req.Key] = set
		}
		for _, pair := range req.ZPairs {
			if _, existed := set[pair.Member]; !existed {
				reply.Int++
			}
			set[pair.Member] = pair.Score
		}
	case nekoRedisZRange:
		set := zsets[req.Key]
		if len(set) == 0 {
			break
		}
		pairs := make([]ZMember, 0, len(set))
		for member, score := range set {
			pairs = append(pairs, ZMember{Member: member, Score: score})
		}
		sort.Slice(pairs, func(i, j int) bool {
			if pairs[i].Score == pairs[j].Score {
				return pairs[i].Member < pairs[j].Member
			}
			return pairs[i].Score < pairs[j].Score
		})
		start, stop, ok := normalizeRedisRange(len(pairs), req.Start, req.Stop)
		if !ok {
			break
		}
		reply.Values = make([]string, 0, stop-start+1)
		for idx := start; idx <= stop; idx++ {
			reply.Values = append(reply.Values, pairs[idx].Member)
		}
	case nekoRedisZCard:
		reply.Int = int64(len(zsets[req.Key]))
	case nekoRedisZRem:
		set := zsets[req.Key]
		if set == nil {
			break
		}
		for _, field := range req.Fields {
			if _, ok := set[field]; ok {
				delete(set, field)
				reply.Int++
			}
		}
		if len(set) == 0 {
			delete(zsets, req.Key)
		}
	}

	req.Reply <- reply
}

func normalizeRedisRange(length, start, stop int) (int, int, bool) {
	if length == 0 {
		return 0, 0, false
	}
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start >= length || stop < start {
		return 0, 0, false
	}
	return start, stop, true
}

func (db *NekoGoDB) requestForKey(key string, req nekoRedisRequest) nekoRedisResponse {
	req.Reply = make(chan nekoRedisResponse, 1)
	db.shards[db.shardFor(key)].ch <- req
	return <-req.Reply
}

func (db *NekoGoDB) shardFor(key string) int {
	if len(db.shards) == 1 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % uint64(len(db.shards)))
}

func (db *NekoGoDB) Set(key, value string, ttl time.Duration) {
	db.requestForKey(key, nekoRedisRequest{
		Op:    nekoRedisSet,
		Key:   key,
		Value: value,
		TTL:   ttl,
	})
}

func (db *NekoGoDB) SetNX(key, value string, ttl time.Duration) bool {
	resp := db.requestForKey(key, nekoRedisRequest{
		Op:    nekoRedisSet,
		Key:   key,
		Value: value,
		TTL:   ttl,
		NX:    true,
	})
	return resp.OK
}

func (db *NekoGoDB) Get(key string) (string, error) {
	resp := db.requestForKey(key, nekoRedisRequest{
		Op:  nekoRedisGet,
		Key: key,
	})
	if resp.IsNil {
		return "", ErrNekoGoDBNil
	}
	return resp.Value, nil
}

func (db *NekoGoDB) Del(keys ...string) int64 {
	var removed int64
	for _, key := range keys {
		resp := db.requestForKey(key, nekoRedisRequest{
			Op:  nekoRedisDel,
			Key: key,
		})
		removed += resp.Int
	}
	return removed
}

func (db *NekoGoDB) HSet(key, field, value string) int64 {
	resp := db.requestForKey(key, nekoRedisRequest{
		Op:    nekoRedisHSet,
		Key:   key,
		Field: field,
		Value: value,
	})
	return resp.Int
}

func (db *NekoGoDB) HGet(key, field string) (string, error) {
	resp := db.requestForKey(key, nekoRedisRequest{
		Op:    nekoRedisHGet,
		Key:   key,
		Field: field,
	})
	if resp.IsNil {
		return "", ErrNekoGoDBNil
	}
	return resp.Value, nil
}

func (db *NekoGoDB) HDel(key string, fields ...string) int64 {
	resp := db.requestForKey(key, nekoRedisRequest{
		Op:     nekoRedisHDel,
		Key:    key,
		Fields: fields,
	})
	return resp.Int
}

func (db *NekoGoDB) ZAdd(key string, pairs ...ZMember) int64 {
	resp := db.requestForKey(key, nekoRedisRequest{
		Op:     nekoRedisZAdd,
		Key:    key,
		ZPairs: pairs,
	})
	return resp.Int
}

func (db *NekoGoDB) ZRange(key string, start, stop int) []string {
	resp := db.requestForKey(key, nekoRedisRequest{
		Op:    nekoRedisZRange,
		Key:   key,
		Start: start,
		Stop:  stop,
	})
	return resp.Values
}

func (db *NekoGoDB) ZCard(key string) int64 {
	resp := db.requestForKey(key, nekoRedisRequest{
		Op:  nekoRedisZCard,
		Key: key,
	})
	return resp.Int
}

func (db *NekoGoDB) ZRem(key string, members ...string) int64 {
	resp := db.requestForKey(key, nekoRedisRequest{
		Op:     nekoRedisZRem,
		Key:    key,
		Fields: members,
	})
	return resp.Int
}

func (db *NekoGoDB) Publish(channel, payload string) int64 {
	return db.pubsub.Publish(channel, payload)
}

func (db *NekoGoDB) Subscribe(channels ...string) *NekoGoDBSubscription {
	return db.pubsub.Subscribe(channels...)
}

func (h *nekoPubSubHub) Publish(channel, payload string) int64 {
	h.mu.RLock()
	targets := make([]*NekoGoDBSubscription, 0, len(h.subs[channel]))
	for sub := range h.subs[channel] {
		targets = append(targets, sub)
	}
	h.mu.RUnlock()

	var delivered int64
	for _, sub := range targets {
		select {
		case <-sub.closed:
		default:
			select {
			case sub.messages <- nekoPubSubMessage{Channel: channel, Payload: payload}:
				delivered++
			default:
			}
		}
	}
	return delivered
}

func (h *nekoPubSubHub) Subscribe(channels ...string) *NekoGoDBSubscription {
	sub := &NekoGoDBSubscription{
		hub:      h,
		channels: append([]string(nil), channels...),
		messages: make(chan nekoPubSubMessage, 256),
		closed:   make(chan struct{}),
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	for _, channel := range channels {
		if h.subs[channel] == nil {
			h.subs[channel] = make(map[*NekoGoDBSubscription]struct{})
		}
		h.subs[channel][sub] = struct{}{}
	}
	return sub
}

func (s *NekoGoDBSubscription) Receive() (string, string, error) {
	msg, ok := <-s.messages
	if !ok {
		return "", "", io.EOF
	}
	return msg.Channel, msg.Payload, nil
}

func (s *NekoGoDBSubscription) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.hub.mu.Lock()
		for _, channel := range s.channels {
			subs := s.hub.subs[channel]
			delete(subs, s)
			if len(subs) == 0 {
				delete(s.hub.subs, channel)
			}
		}
		s.hub.mu.Unlock()
		close(s.messages)
	})
	return nil
}

func NewNekoGoDBServer(cfg Config) *NekoGoDBServer {
	return &NekoGoDBServer{
		cfg:   cfg,
		store: NewNekoGoDB(cfg),
	}
}

func RunNekoGoDB(ctx context.Context, cfg Config) error {
	addr := cfg.RedisAddr
	if strings.TrimSpace(addr) == "" {
		addr = DefaultAddr
	}
	cfg.RedisAddr = addr

	server := NewNekoGoDBServer(cfg)
	listener, err := listenWithTuning(ctx, cfg, addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	errCh := make(chan error, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					errCh <- nil
				default:
					errCh <- err
				}
				return
			}
			go server.handleConn(conn)
		}
	}()

	select {
	case <-ctx.Done():
		_ = listener.Close()
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *NekoGoDBServer) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, 16<<10)
	writer := bufio.NewWriterSize(conn, 16<<10)

	for {
		args, err := readRESPArray(reader)
		if err != nil {
			return
		}
		if len(args) == 0 {
			if err := writeRESPError(writer, "empty command"); err != nil {
				return
			}
			continue
		}

		cmd := strings.ToUpper(args[0])
		switch cmd {
		case "PING":
			if err := writeRESPSimpleString(writer, "PONG"); err != nil {
				return
			}
		case "WKS":
			if err := writeRESPInteger(writer, int64(s.store.Shards())); err != nil {
				return
			}
		case "SET":
			if err := s.handleSet(writer, args); err != nil {
				return
			}
		case "GET":
			if len(args) != 2 {
				if err := writeRESPError(writer, "ERR wrong number of arguments for GET"); err != nil {
					return
				}
				continue
			}
			value, err := s.store.Get(args[1])
			if err != nil {
				if errors.Is(err, ErrNekoGoDBNil) {
					if err := writeRESPNil(writer); err != nil {
						return
					}
				} else if err := writeRESPError(writer, err.Error()); err != nil {
					return
				}
				continue
			}
			if err := writeRESPBulkString(writer, value); err != nil {
				return
			}
		case "DEL":
			if len(args) < 2 {
				if err := writeRESPError(writer, "ERR wrong number of arguments for DEL"); err != nil {
					return
				}
				continue
			}
			if err := writeRESPInteger(writer, s.store.Del(args[1:]...)); err != nil {
				return
			}
		case "HSET":
			if len(args) != 4 {
				if err := writeRESPError(writer, "ERR wrong number of arguments for HSET"); err != nil {
					return
				}
				continue
			}
			if err := writeRESPInteger(writer, s.store.HSet(args[1], args[2], args[3])); err != nil {
				return
			}
		case "HGET":
			if len(args) != 3 {
				if err := writeRESPError(writer, "ERR wrong number of arguments for HGET"); err != nil {
					return
				}
				continue
			}
			value, err := s.store.HGet(args[1], args[2])
			if err != nil {
				if errors.Is(err, ErrNekoGoDBNil) {
					if err := writeRESPNil(writer); err != nil {
						return
					}
				} else if err := writeRESPError(writer, err.Error()); err != nil {
					return
				}
				continue
			}
			if err := writeRESPBulkString(writer, value); err != nil {
				return
			}
		case "HDEL":
			if len(args) < 3 {
				if err := writeRESPError(writer, "ERR wrong number of arguments for HDEL"); err != nil {
					return
				}
				continue
			}
			if err := writeRESPInteger(writer, s.store.HDel(args[1], args[2:]...)); err != nil {
				return
			}
		case "ZADD":
			if len(args) < 4 || len(args)%2 != 0 {
				if err := writeRESPError(writer, "ERR wrong number of arguments for ZADD"); err != nil {
					return
				}
				continue
			}
			pairs := make([]ZMember, 0, (len(args)-2)/2)
			for i := 2; i < len(args); i += 2 {
				score, err := strconv.ParseFloat(args[i], 64)
				if err != nil {
					if err := writeRESPError(writer, "ERR invalid score"); err != nil {
						return
					}
					pairs = nil
					break
				}
				pairs = append(pairs, ZMember{Score: score, Member: args[i+1]})
			}
			if pairs == nil {
				continue
			}
			if err := writeRESPInteger(writer, s.store.ZAdd(args[1], pairs...)); err != nil {
				return
			}
		case "ZRANGE":
			if len(args) != 4 {
				if err := writeRESPError(writer, "ERR wrong number of arguments for ZRANGE"); err != nil {
					return
				}
				continue
			}
			start, err := strconv.Atoi(args[2])
			if err != nil {
				if err := writeRESPError(writer, "ERR invalid start"); err != nil {
					return
				}
				continue
			}
			stop, err := strconv.Atoi(args[3])
			if err != nil {
				if err := writeRESPError(writer, "ERR invalid stop"); err != nil {
					return
				}
				continue
			}
			if err := writeRESPArray(writer, s.store.ZRange(args[1], start, stop)); err != nil {
				return
			}
		case "ZCARD":
			if len(args) != 2 {
				if err := writeRESPError(writer, "ERR wrong number of arguments for ZCARD"); err != nil {
					return
				}
				continue
			}
			if err := writeRESPInteger(writer, s.store.ZCard(args[1])); err != nil {
				return
			}
		case "ZREM":
			if len(args) < 3 {
				if err := writeRESPError(writer, "ERR wrong number of arguments for ZREM"); err != nil {
					return
				}
				continue
			}
			if err := writeRESPInteger(writer, s.store.ZRem(args[1], args[2:]...)); err != nil {
				return
			}
		case "PUBLISH":
			if len(args) != 3 {
				if err := writeRESPError(writer, "ERR wrong number of arguments for PUBLISH"); err != nil {
					return
				}
				continue
			}
			if err := writeRESPInteger(writer, s.store.Publish(args[1], args[2])); err != nil {
				return
			}
		case "SUBSCRIBE":
			if len(args) < 2 {
				_ = writeRESPError(writer, "ERR wrong number of arguments for SUBSCRIBE")
				return
			}
			s.serveSubscription(writer, s.store.Subscribe(args[1:]...))
			return
		default:
			if err := writeRESPError(writer, "ERR unknown command "+cmd); err != nil {
				return
			}
		}
	}
}

func (s *NekoGoDBServer) handleSet(writer *bufio.Writer, args []string) error {
	if len(args) < 3 {
		return writeRESPError(writer, "ERR wrong number of arguments for SET")
	}

	ttl := time.Duration(0)
	nx := false
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "PX":
			if i+1 >= len(args) {
				return writeRESPError(writer, "ERR PX requires milliseconds")
			}
			ms, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return writeRESPError(writer, "ERR invalid PX value")
			}
			ttl = time.Duration(ms) * time.Millisecond
			i++
		case "EX":
			if i+1 >= len(args) {
				return writeRESPError(writer, "ERR EX requires seconds")
			}
			sec, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return writeRESPError(writer, "ERR invalid EX value")
			}
			ttl = time.Duration(sec) * time.Second
			i++
		case "NX":
			nx = true
		default:
			return writeRESPError(writer, "ERR unsupported SET option "+args[i])
		}
	}

	if nx {
		if ok := s.store.SetNX(args[1], args[2], ttl); !ok {
			return writeRESPNil(writer)
		}
		return writeRESPSimpleString(writer, "OK")
	}

	s.store.Set(args[1], args[2], ttl)
	return writeRESPSimpleString(writer, "OK")
}

func (s *NekoGoDBServer) serveSubscription(writer *bufio.Writer, sub *NekoGoDBSubscription) {
	defer sub.Close()

	for idx, channel := range sub.channels {
		if err := writeRESPPushArray(writer, "subscribe", channel, strconv.Itoa(idx+1)); err != nil {
			return
		}
	}

	for {
		channel, payload, err := sub.Receive()
		if err != nil {
			return
		}
		if err := writeRESPPushArray(writer, "message", channel, payload); err != nil {
			return
		}
	}
}

func readRESPArray(reader *bufio.Reader) ([]string, error) {
	line, err := readRESPLine(reader)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, errors.New("invalid RESP array")
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		header, err := readRESPLine(reader)
		if err != nil {
			return nil, err
		}
		if len(header) == 0 || header[0] != '$' {
			return nil, errors.New("invalid RESP bulk string")
		}
		size, err := strconv.Atoi(header[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

func readRESPLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadSlice('\n')
	if err == nil {
		return trimRESPLine(line), nil
	}
	if !errors.Is(err, bufio.ErrBufferFull) {
		return "", err
	}

	buf := append([]byte(nil), line...)
	for {
		chunk, readErr := reader.ReadSlice('\n')
		buf = append(buf, chunk...)
		if readErr == nil {
			return trimRESPLine(buf), nil
		}
		if !errors.Is(readErr, bufio.ErrBufferFull) {
			return "", readErr
		}
	}
}

func trimRESPLine(line []byte) string {
	n := len(line)
	if n > 0 && line[n-1] == '\n' {
		n--
	}
	if n > 0 && line[n-1] == '\r' {
		n--
	}
	return string(line[:n])
}

func writeRESPSimpleString(writer *bufio.Writer, value string) error {
	if _, err := writer.WriteString("+" + value + "\r\n"); err != nil {
		return err
	}
	return writer.Flush()
}

func writeRESPError(writer *bufio.Writer, value string) error {
	if _, err := writer.WriteString("-" + value + "\r\n"); err != nil {
		return err
	}
	return writer.Flush()
}

func writeRESPInteger(writer *bufio.Writer, value int64) error {
	if _, err := writer.WriteString(":" + strconv.FormatInt(value, 10) + "\r\n"); err != nil {
		return err
	}
	return writer.Flush()
}

func writeRESPBulkString(writer *bufio.Writer, value string) error {
	if _, err := writer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)); err != nil {
		return err
	}
	return writer.Flush()
}

func writeRESPNil(writer *bufio.Writer) error {
	if _, err := writer.WriteString("$-1\r\n"); err != nil {
		return err
	}
	return writer.Flush()
}

func writeRESPArray(writer *bufio.Writer, values []string) error {
	if _, err := writer.WriteString("*" + strconv.Itoa(len(values)) + "\r\n"); err != nil {
		return err
	}
	for _, value := range values {
		if _, err := writer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func writeRESPPushArray(writer *bufio.Writer, values ...string) error {
	return writeRESPArray(writer, values)
}

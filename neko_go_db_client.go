package neko_go_db

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type respConn struct {
	net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

type respReply struct {
	Kind  byte
	Text  string
	Int   int64
	Array []respReply
	IsNil bool
}

type NekoGoDBClient struct {
	addr    string
	timeout time.Duration
	pool    chan *respConn
}

type NekoGoDBClientSubscription struct {
	conn   *respConn
	closed sync.Once
}

func NewNekoGoDBClient(cfg Config) (*NekoGoDBClient, error) {
	if strings.TrimSpace(cfg.RedisAddr) == "" {
		return nil, nil
	}

	client := &NekoGoDBClient{
		addr:    cfg.RedisAddr,
		timeout: cfg.RequestTimeout,
		pool:    make(chan *respConn, cfg.RedisConnPool),
	}
	if err := client.Ping(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func (c *NekoGoDBClient) Close() error {
	if c == nil {
		return nil
	}
	close(c.pool)
	for conn := range c.pool {
		_ = conn.Close()
	}
	return nil
}

func (c *NekoGoDBClient) Ping() error {
	reply, err := c.do("PING")
	if err != nil {
		return err
	}
	if reply.Kind != '+' || reply.Text != "PONG" {
		return errors.New("invalid PING response")
	}
	return nil
}

func (c *NekoGoDBClient) Set(key, value string, ttl time.Duration) error {
	args := []string{"SET", key, value}
	if ttl > 0 {
		args = append(args, "PX", strconv.FormatInt(ttl.Milliseconds(), 10))
	}
	reply, err := c.do(args...)
	if err != nil {
		return err
	}
	if reply.Kind != '+' || reply.Text != "OK" {
		return errors.New("SET failed")
	}
	return nil
}

func (c *NekoGoDBClient) SetNX(key, value string, ttl time.Duration) (bool, error) {
	args := []string{"SET", key, value}
	if ttl > 0 {
		args = append(args, "PX", strconv.FormatInt(ttl.Milliseconds(), 10))
	}
	args = append(args, "NX")
	reply, err := c.do(args...)
	if err != nil {
		return false, err
	}
	if reply.IsNil {
		return false, nil
	}
	return reply.Kind == '+' && reply.Text == "OK", nil
}

func (c *NekoGoDBClient) Get(key string) (string, error) {
	reply, err := c.do("GET", key)
	if err != nil {
		return "", err
	}
	if reply.IsNil {
		return "", ErrNekoGoDBNil
	}
	return reply.Text, nil
}

func (c *NekoGoDBClient) Del(keys ...string) (int64, error) {
	args := append([]string{"DEL"}, keys...)
	reply, err := c.do(args...)
	if err != nil {
		return 0, err
	}
	return reply.Int, nil
}

func (c *NekoGoDBClient) HSet(key, field, value string) (int64, error) {
	reply, err := c.do("HSET", key, field, value)
	if err != nil {
		return 0, err
	}
	return reply.Int, nil
}

func (c *NekoGoDBClient) HGet(key, field string) (string, error) {
	reply, err := c.do("HGET", key, field)
	if err != nil {
		return "", err
	}
	if reply.IsNil {
		return "", ErrNekoGoDBNil
	}
	return reply.Text, nil
}

func (c *NekoGoDBClient) HDel(key string, fields ...string) (int64, error) {
	args := append([]string{"HDEL", key}, fields...)
	reply, err := c.do(args...)
	if err != nil {
		return 0, err
	}
	return reply.Int, nil
}

func (c *NekoGoDBClient) ZAdd(key string, pairs ...ZMember) (int64, error) {
	args := []string{"ZADD", key}
	for _, pair := range pairs {
		args = append(args, strconv.FormatFloat(pair.Score, 'f', -1, 64), pair.Member)
	}
	reply, err := c.do(args...)
	if err != nil {
		return 0, err
	}
	return reply.Int, nil
}

func (c *NekoGoDBClient) ZRange(key string, start, stop int) ([]string, error) {
	reply, err := c.do("ZRANGE", key, strconv.Itoa(start), strconv.Itoa(stop))
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, len(reply.Array))
	for _, item := range reply.Array {
		if item.IsNil {
			values = append(values, "")
			continue
		}
		values = append(values, item.Text)
	}
	return values, nil
}

func (c *NekoGoDBClient) ZCard(key string) (int64, error) {
	reply, err := c.do("ZCARD", key)
	if err != nil {
		return 0, err
	}
	return reply.Int, nil
}

func (c *NekoGoDBClient) ZRem(key string, members ...string) (int64, error) {
	args := append([]string{"ZREM", key}, members...)
	reply, err := c.do(args...)
	if err != nil {
		return 0, err
	}
	return reply.Int, nil
}

func (c *NekoGoDBClient) Publish(channel, payload string) (int64, error) {
	reply, err := c.do("PUBLISH", channel, payload)
	if err != nil {
		return 0, err
	}
	return reply.Int, nil
}

func (c *NekoGoDBClient) WorkerCount() (int64, error) {
	reply, err := c.do("WKS")
	if err != nil {
		return 0, err
	}
	return reply.Int, nil
}

func (c *NekoGoDBClient) Subscribe(channels ...string) (*NekoGoDBClientSubscription, error) {
	conn, err := c.dial()
	if err != nil {
		return nil, err
	}
	if err := writeRESPCommand(conn.writer, append([]string{"SUBSCRIBE"}, channels...)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	for range channels {
		if _, err := readRESPReply(conn.reader); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	_ = conn.SetDeadline(time.Time{})
	return &NekoGoDBClientSubscription{conn: conn}, nil
}

func (s *NekoGoDBClientSubscription) Receive() (string, string, error) {
	reply, err := readRESPReply(s.conn.reader)
	if err != nil {
		return "", "", err
	}
	if reply.Kind != '*' || len(reply.Array) < 3 {
		return "", "", errors.New("invalid subscription message")
	}
	if reply.Array[0].Text != "message" {
		return "", "", errors.New("unexpected subscription frame")
	}
	return reply.Array[1].Text, reply.Array[2].Text, nil
}

func (s *NekoGoDBClientSubscription) Close() error {
	s.closed.Do(func() {
		_ = s.conn.Close()
	})
	return nil
}

func (c *NekoGoDBClient) do(args ...string) (respReply, error) {
	conn, err := c.getConn()
	if err != nil {
		return respReply{}, err
	}
	defer c.putConn(conn)

	if err := writeRESPCommand(conn.writer, args); err != nil {
		_ = conn.Close()
		return respReply{}, err
	}

	reply, err := readRESPReply(conn.reader)
	if err != nil {
		_ = conn.Close()
		return respReply{}, err
	}
	if reply.Kind == '-' {
		return respReply{}, errors.New(reply.Text)
	}
	return reply, nil
}

func (c *NekoGoDBClient) doStatus(args ...string) (string, error) {
	conn, err := c.getConn()
	if err != nil {
		return "", err
	}
	keep := false
	defer func() {
		if keep {
			c.putConn(conn)
		}
	}()

	if err := writeRESPCommand(conn.writer, args); err != nil {
		_ = conn.Close()
		return "", err
	}

	reply, err := readRESPStatus(conn.reader)
	if err != nil {
		_ = conn.Close()
		return "", err
	}
	keep = true
	return reply, nil
}

func (c *NekoGoDBClient) doStatusOrNil(args ...string) (string, bool, error) {
	conn, err := c.getConn()
	if err != nil {
		return "", false, err
	}
	keep := false
	defer func() {
		if keep {
			c.putConn(conn)
		}
	}()

	if err := writeRESPCommand(conn.writer, args); err != nil {
		_ = conn.Close()
		return "", false, err
	}

	reply, isNil, err := readRESPStatusOrNil(conn.reader)
	if err != nil {
		_ = conn.Close()
		return "", false, err
	}
	keep = true
	return reply, isNil, nil
}

func (c *NekoGoDBClient) doInt(args ...string) (int64, error) {
	conn, err := c.getConn()
	if err != nil {
		return 0, err
	}
	keep := false
	defer func() {
		if keep {
			c.putConn(conn)
		}
	}()

	if err := writeRESPCommand(conn.writer, args); err != nil {
		_ = conn.Close()
		return 0, err
	}

	reply, err := readRESPInteger(conn.reader)
	if err != nil {
		_ = conn.Close()
		return 0, err
	}
	keep = true
	return reply, nil
}

func (c *NekoGoDBClient) doBulkString(args ...string) (string, bool, error) {
	conn, err := c.getConn()
	if err != nil {
		return "", false, err
	}
	keep := false
	defer func() {
		if keep {
			c.putConn(conn)
		}
	}()

	if err := writeRESPCommand(conn.writer, args); err != nil {
		_ = conn.Close()
		return "", false, err
	}

	reply, isNil, err := readRESPBulkString(conn.reader)
	if err != nil {
		_ = conn.Close()
		return "", false, err
	}
	keep = true
	return reply, isNil, nil
}

func (c *NekoGoDBClient) doArrayStrings(args ...string) ([]string, error) {
	conn, err := c.getConn()
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if keep {
			c.putConn(conn)
		}
	}()

	if err := writeRESPCommand(conn.writer, args); err != nil {
		_ = conn.Close()
		return nil, err
	}

	reply, err := readRESPArrayStrings(conn.reader)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	keep = true
	return reply, nil
}

func (c *NekoGoDBClient) getConn() (*respConn, error) {
	select {
	case conn := <-c.pool:
		if conn == nil {
			return c.dial()
		}
		_ = conn.SetDeadline(time.Now().Add(c.timeout))
		return conn, nil
	default:
		return c.dial()
	}
}

func (c *NekoGoDBClient) putConn(conn *respConn) {
	if conn == nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})
	select {
	case c.pool <- conn:
	default:
		_ = conn.Close()
	}
}

func (c *NekoGoDBClient) dial() (*respConn, error) {
	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(c.timeout))
	return &respConn{
		Conn:   conn,
		reader: bufio.NewReaderSize(conn, 16<<10),
		writer: bufio.NewWriterSize(conn, 16<<10),
	}, nil
}

func writeRESPCommand(writer *bufio.Writer, args []string) error {
	if err := writeRESPCountLine(writer, '*', len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if err := writeRESPCountLine(writer, '$', len(arg)); err != nil {
			return err
		}
		if _, err := writer.WriteString(arg); err != nil {
			return err
		}
		if _, err := writer.WriteString("\r\n"); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func writeRESPCountLine(writer *bufio.Writer, prefix byte, value int) error {
	var buf [32]byte
	buf[0] = prefix
	out := strconv.AppendInt(buf[:1], int64(value), 10)
	out = append(out, '\r', '\n')
	_, err := writer.Write(out)
	return err
}

func readRESPStatus(reader *bufio.Reader) (string, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	switch prefix {
	case '+':
		return readRESPLine(reader)
	case '-':
		line, err := readRESPLine(reader)
		if err != nil {
			return "", err
		}
		return "", errors.New(line)
	default:
		return "", errors.New("invalid RESP status reply")
	}
}

func readRESPStatusOrNil(reader *bufio.Reader) (string, bool, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return "", false, err
	}
	switch prefix {
	case '+':
		line, err := readRESPLine(reader)
		return line, false, err
	case '$':
		line, err := readRESPLine(reader)
		if err != nil {
			return "", false, err
		}
		size, err := strconv.Atoi(line)
		if err != nil {
			return "", false, err
		}
		if size < 0 {
			return "", true, nil
		}
		value, err := readRESPBulkPayload(reader, size)
		return value, false, err
	case '-':
		line, err := readRESPLine(reader)
		if err != nil {
			return "", false, err
		}
		return "", false, errors.New(line)
	default:
		return "", false, errors.New("invalid RESP status-or-nil reply")
	}
}

func readRESPInteger(reader *bufio.Reader) (int64, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}
	switch prefix {
	case ':':
		line, err := readRESPLine(reader)
		if err != nil {
			return 0, err
		}
		return strconv.ParseInt(line, 10, 64)
	case '-':
		line, err := readRESPLine(reader)
		if err != nil {
			return 0, err
		}
		return 0, errors.New(line)
	default:
		return 0, errors.New("invalid RESP integer reply")
	}
}

func readRESPBulkString(reader *bufio.Reader) (string, bool, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return "", false, err
	}
	switch prefix {
	case '$':
		line, err := readRESPLine(reader)
		if err != nil {
			return "", false, err
		}
		size, err := strconv.Atoi(line)
		if err != nil {
			return "", false, err
		}
		if size < 0 {
			return "", true, nil
		}
		value, err := readRESPBulkPayload(reader, size)
		return value, false, err
	case '-':
		line, err := readRESPLine(reader)
		if err != nil {
			return "", false, err
		}
		return "", false, errors.New(line)
	default:
		return "", false, errors.New("invalid RESP bulk string reply")
	}
}

func readRESPArrayStrings(reader *bufio.Reader) ([]string, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix == '-' {
		line, err := readRESPLine(reader)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(line)
	}
	if prefix != '*' {
		return nil, errors.New("invalid RESP array reply")
	}

	line, err := readRESPLine(reader)
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}

	values := make([]string, 0, count)
	for i := 0; i < count; i++ {
		value, _, err := readRESPStringElement(reader)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func readRESPPubSubMessage(reader *bufio.Reader) (string, string, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return "", "", err
	}
	if prefix == '-' {
		line, err := readRESPLine(reader)
		if err != nil {
			return "", "", err
		}
		return "", "", errors.New(line)
	}
	if prefix != '*' {
		return "", "", errors.New("invalid subscription message")
	}

	line, err := readRESPLine(reader)
	if err != nil {
		return "", "", err
	}
	count, err := strconv.Atoi(line)
	if err != nil {
		return "", "", err
	}
	if count < 3 {
		return "", "", errors.New("invalid subscription message")
	}

	frameType, _, err := readRESPStringElement(reader)
	if err != nil {
		return "", "", err
	}
	if frameType != "message" {
		return "", "", errors.New("unexpected subscription frame")
	}

	channel, _, err := readRESPStringElement(reader)
	if err != nil {
		return "", "", err
	}
	payload, _, err := readRESPStringElement(reader)
	if err != nil {
		return "", "", err
	}
	for i := 3; i < count; i++ {
		if _, _, err := readRESPStringElement(reader); err != nil {
			return "", "", err
		}
	}
	return channel, payload, nil
}

func readRESPStringElement(reader *bufio.Reader) (string, bool, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return "", false, err
	}
	switch prefix {
	case '$':
		line, err := readRESPLine(reader)
		if err != nil {
			return "", false, err
		}
		size, err := strconv.Atoi(line)
		if err != nil {
			return "", false, err
		}
		if size < 0 {
			return "", true, nil
		}
		value, err := readRESPBulkPayload(reader, size)
		return value, false, err
	case '+':
		line, err := readRESPLine(reader)
		return line, false, err
	case ':':
		line, err := readRESPLine(reader)
		return line, false, err
	case '-':
		line, err := readRESPLine(reader)
		if err != nil {
			return "", false, err
		}
		return "", false, errors.New(line)
	default:
		return "", false, errors.New("invalid RESP string element")
	}
}

func readRESPBulkPayload(reader *bufio.Reader, size int) (string, error) {
	buf := make([]byte, size)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", err
	}
	if _, err := reader.ReadByte(); err != nil {
		return "", err
	}
	if _, err := reader.ReadByte(); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readRESPReply(reader *bufio.Reader) (respReply, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return respReply{}, err
	}

	switch prefix {
	case '+', '-':
		line, err := readRESPLine(reader)
		if err != nil {
			return respReply{}, err
		}
		return respReply{Kind: prefix, Text: line}, nil
	case ':':
		line, err := readRESPLine(reader)
		if err != nil {
			return respReply{}, err
		}
		value, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return respReply{}, err
		}
		return respReply{Kind: prefix, Int: value}, nil
	case '$':
		line, err := readRESPLine(reader)
		if err != nil {
			return respReply{}, err
		}
		size, err := strconv.Atoi(line)
		if err != nil {
			return respReply{}, err
		}
		if size < 0 {
			return respReply{Kind: '$', IsNil: true}, nil
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return respReply{}, err
		}
		return respReply{Kind: '$', Text: string(buf[:size])}, nil
	case '*':
		line, err := readRESPLine(reader)
		if err != nil {
			return respReply{}, err
		}
		count, err := strconv.Atoi(line)
		if err != nil {
			return respReply{}, err
		}
		reply := respReply{Kind: '*', Array: make([]respReply, 0, count)}
		for i := 0; i < count; i++ {
			item, err := readRESPReply(reader)
			if err != nil {
				return respReply{}, err
			}
			reply.Array = append(reply.Array, item)
		}
		return reply, nil
	default:
		return respReply{}, errors.New("unknown RESP prefix")
	}
}

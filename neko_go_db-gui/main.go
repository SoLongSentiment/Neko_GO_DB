package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	fyneapp "fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"

	redis "neko_go_db"
)

type gui struct {
	app    fyne.App
	window fyne.Window

	addrEntry   *widget.Entry
	statusLabel *widget.Label
	logEntry    *widget.Entry

	stringKey   *widget.Entry
	stringValue *widget.Entry
	stringTTL   *widget.Entry

	hashKey   *widget.Entry
	hashField *widget.Entry
	hashValue *widget.Entry

	zsetKey    *widget.Entry
	zsetMember *widget.Entry
	zsetScore  *widget.Entry
	zsetStart  *widget.Entry
	zsetStop   *widget.Entry

	channelEntry *widget.Entry
	payloadEntry *widget.Entry

	client *redis.NekoGoDBClient
	sub    *redis.NekoGoDBClientSubscription
}

func main() {
	g := &gui{}
	g.app = fyneapp.NewWithID("neko_go_db_gui")
	g.window = g.app.NewWindow("NekoGoDB GUI")
	g.window.Resize(fyne.NewSize(1280, 860))
	g.window.SetContent(g.build())
	g.window.ShowAndRun()
}

func (g *gui) build() fyne.CanvasObject {
	g.addrEntry = widget.NewEntry()
	g.addrEntry.SetText(redis.DefaultAddr)
	g.statusLabel = widget.NewLabel("Disconnected")

	g.logEntry = widget.NewMultiLineEntry()
	g.logEntry.Disable()
	g.logEntry.Wrapping = fyne.TextWrapWord
	g.logEntry.SetMinRowsVisible(16)

	g.stringKey = widget.NewEntry()
	g.stringKey.SetText("demo:key")
	g.stringValue = widget.NewEntry()
	g.stringValue.SetText("hello")
	g.stringTTL = widget.NewEntry()
	g.stringTTL.SetText("0")

	g.hashKey = widget.NewEntry()
	g.hashKey.SetText("demo:hash")
	g.hashField = widget.NewEntry()
	g.hashField.SetText("field")
	g.hashValue = widget.NewEntry()
	g.hashValue.SetText("value")

	g.zsetKey = widget.NewEntry()
	g.zsetKey.SetText("demo:zset")
	g.zsetMember = widget.NewEntry()
	g.zsetMember.SetText("alice")
	g.zsetScore = widget.NewEntry()
	g.zsetScore.SetText("1000")
	g.zsetStart = widget.NewEntry()
	g.zsetStart.SetText("0")
	g.zsetStop = widget.NewEntry()
	g.zsetStop.SetText("-1")

	g.channelEntry = widget.NewEntry()
	g.channelEntry.SetText("events:demo")
	g.payloadEntry = widget.NewEntry()
	g.payloadEntry.SetText(`{"hello":"world"}`)

	top := container.NewVBox(
		widget.NewLabelWithStyle("NekoGoDB GUI", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabel("Address"),
		g.addrEntry,
		container.NewGridWithColumns(2,
			widget.NewButton("Connect", func() { go g.connect() }),
			widget.NewButton("Disconnect", func() { g.disconnect() }),
		),
		g.statusLabel,
	)

	tabs := container.NewAppTabs(
		container.NewTabItem("Strings", g.buildStringsTab()),
		container.NewTabItem("Hashes", g.buildHashesTab()),
		container.NewTabItem("ZSets", g.buildZSetTab()),
		container.NewTabItem("PubSub", g.buildPubSubTab()),
		container.NewTabItem("Server", g.buildServerTab()),
	)

	return container.NewBorder(top, nil, nil, nil, container.NewVSplit(tabs, g.logEntry))
}

func (g *gui) buildStringsTab() fyne.CanvasObject {
	return container.NewVBox(
		widget.NewLabel("Key"), g.stringKey,
		widget.NewLabel("Value"), g.stringValue,
		widget.NewLabel("TTL ms"), g.stringTTL,
		container.NewGridWithColumns(4,
			widget.NewButton("SET", func() { go g.setString() }),
			widget.NewButton("SET NX", func() { go g.setNX() }),
			widget.NewButton("GET", func() { go g.getString() }),
			widget.NewButton("DEL", func() { go g.delString() }),
		),
	)
}

func (g *gui) buildHashesTab() fyne.CanvasObject {
	return container.NewVBox(
		widget.NewLabel("Hash key"), g.hashKey,
		widget.NewLabel("Field"), g.hashField,
		widget.NewLabel("Value"), g.hashValue,
		container.NewGridWithColumns(3,
			widget.NewButton("HSET", func() { go g.hset() }),
			widget.NewButton("HGET", func() { go g.hget() }),
			widget.NewButton("HDEL", func() { go g.hdel() }),
		),
	)
}

func (g *gui) buildZSetTab() fyne.CanvasObject {
	return container.NewVBox(
		widget.NewLabel("ZSet key"), g.zsetKey,
		widget.NewLabel("Member"), g.zsetMember,
		widget.NewLabel("Score"), g.zsetScore,
		container.NewGridWithColumns(2,
			container.NewVBox(widget.NewLabel("Start"), g.zsetStart),
			container.NewVBox(widget.NewLabel("Stop"), g.zsetStop),
		),
		container.NewGridWithColumns(4,
			widget.NewButton("ZADD", func() { go g.zadd() }),
			widget.NewButton("ZRANGE", func() { go g.zrangeCmd() }),
			widget.NewButton("ZCARD", func() { go g.zcard() }),
			widget.NewButton("ZREM", func() { go g.zrem() }),
		),
	)
}

func (g *gui) buildPubSubTab() fyne.CanvasObject {
	return container.NewVBox(
		widget.NewLabel("Channel"), g.channelEntry,
		widget.NewLabel("Payload"), g.payloadEntry,
		container.NewGridWithColumns(3,
			widget.NewButton("SUBSCRIBE", func() { go g.subscribe() }),
			widget.NewButton("UNSUBSCRIBE", func() { g.unsubscribe() }),
			widget.NewButton("PUBLISH", func() { go g.publish() }),
		),
	)
}

func (g *gui) buildServerTab() fyne.CanvasObject {
	return container.NewVBox(
		container.NewGridWithColumns(2,
			widget.NewButton("PING", func() { go g.ping() }),
			widget.NewButton("WKS", func() { go g.workerCount() }),
		),
	)
}

func (g *gui) connect() {
	cfg := redis.DefaultConfig()
	cfg.RedisAddr = strings.TrimSpace(g.addrEntry.Text)
	client, err := redis.NewNekoGoDBClient(cfg)
	if err != nil {
		g.showError(err)
		g.setStatus("Connect failed")
		return
	}
	if g.client != nil {
		_ = g.client.Close()
	}
	g.client = client
	g.setStatus("Connected")
	g.appendLog("connected to " + cfg.RedisAddr)
}

func (g *gui) disconnect() {
	g.unsubscribe()
	if g.client != nil {
		_ = g.client.Close()
		g.client = nil
	}
	g.setStatus("Disconnected")
}

func (g *gui) requireClient() (*redis.NekoGoDBClient, bool) {
	if g.client == nil {
		g.showError(fmt.Errorf("connect first"))
		return nil, false
	}
	return g.client, true
}

func (g *gui) setString() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	ttl, err := parseDurationMs(g.stringTTL.Text)
	if err != nil {
		g.showError(err)
		return
	}
	if err := client.Set(strings.TrimSpace(g.stringKey.Text), g.stringValue.Text, ttl); err != nil {
		g.showError(err)
		return
	}
	g.appendLog("SET ok")
}

func (g *gui) setNX() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	ttl, err := parseDurationMs(g.stringTTL.Text)
	if err != nil {
		g.showError(err)
		return
	}
	set, err := client.SetNX(strings.TrimSpace(g.stringKey.Text), g.stringValue.Text, ttl)
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog(fmt.Sprintf("SETNX => %v", set))
}

func (g *gui) getString() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	value, err := client.Get(strings.TrimSpace(g.stringKey.Text))
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog("GET => " + value)
}

func (g *gui) delString() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	removed, err := client.Del(strings.TrimSpace(g.stringKey.Text))
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog(fmt.Sprintf("DEL => %d", removed))
}

func (g *gui) hset() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	affected, err := client.HSet(strings.TrimSpace(g.hashKey.Text), strings.TrimSpace(g.hashField.Text), g.hashValue.Text)
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog(fmt.Sprintf("HSET => %d", affected))
}

func (g *gui) hget() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	value, err := client.HGet(strings.TrimSpace(g.hashKey.Text), strings.TrimSpace(g.hashField.Text))
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog("HGET => " + value)
}

func (g *gui) hdel() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	removed, err := client.HDel(strings.TrimSpace(g.hashKey.Text), strings.TrimSpace(g.hashField.Text))
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog(fmt.Sprintf("HDEL => %d", removed))
}

func (g *gui) zadd() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	score, err := strconv.ParseFloat(strings.TrimSpace(g.zsetScore.Text), 64)
	if err != nil {
		g.showError(err)
		return
	}
	added, err := client.ZAdd(strings.TrimSpace(g.zsetKey.Text), redis.ZMember{
		Score:  score,
		Member: strings.TrimSpace(g.zsetMember.Text),
	})
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog(fmt.Sprintf("ZADD => %d", added))
}

func (g *gui) zrangeCmd() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	start, err := strconv.Atoi(strings.TrimSpace(g.zsetStart.Text))
	if err != nil {
		g.showError(err)
		return
	}
	stop, err := strconv.Atoi(strings.TrimSpace(g.zsetStop.Text))
	if err != nil {
		g.showError(err)
		return
	}
	values, err := client.ZRange(strings.TrimSpace(g.zsetKey.Text), start, stop)
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog("ZRANGE => " + strings.Join(values, ", "))
}

func (g *gui) zcard() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	count, err := client.ZCard(strings.TrimSpace(g.zsetKey.Text))
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog(fmt.Sprintf("ZCARD => %d", count))
}

func (g *gui) zrem() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	removed, err := client.ZRem(strings.TrimSpace(g.zsetKey.Text), strings.TrimSpace(g.zsetMember.Text))
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog(fmt.Sprintf("ZREM => %d", removed))
}

func (g *gui) subscribe() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	g.unsubscribe()
	sub, err := client.Subscribe(strings.TrimSpace(g.channelEntry.Text))
	if err != nil {
		g.showError(err)
		return
	}
	g.sub = sub
	g.appendLog("SUBSCRIBE ok")
	go func() {
		for {
			channel, payload, err := sub.Receive()
			if err != nil {
				fyne.Do(func() {
					g.appendLog("subscription ended: " + err.Error())
				})
				return
			}
			fyne.Do(func() {
				g.appendLog(fmt.Sprintf("message [%s] %s", channel, payload))
			})
		}
	}()
}

func (g *gui) unsubscribe() {
	if g.sub != nil {
		_ = g.sub.Close()
		g.sub = nil
		g.appendLog("UNSUBSCRIBE")
	}
}

func (g *gui) publish() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	delivered, err := client.Publish(strings.TrimSpace(g.channelEntry.Text), g.payloadEntry.Text)
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog(fmt.Sprintf("PUBLISH => %d", delivered))
}

func (g *gui) ping() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	if err := client.Ping(); err != nil {
		g.showError(err)
		return
	}
	g.appendLog("PING => PONG")
}

func (g *gui) workerCount() {
	client, ok := g.requireClient()
	if !ok {
		return
	}
	count, err := client.WorkerCount()
	if err != nil {
		g.showError(err)
		return
	}
	g.appendLog(fmt.Sprintf("WKS => %d", count))
}

func (g *gui) setStatus(text string) {
	fyne.Do(func() {
		g.statusLabel.SetText(text)
	})
}

func (g *gui) appendLog(line string) {
	fyne.Do(func() {
		text := g.logEntry.Text
		if text != "" {
			text += "\n"
		}
		g.logEntry.SetText(text + line)
	})
}

func (g *gui) showError(err error) {
	fyne.Do(func() {
		dialog.ShowError(err, g.window)
	})
}

func parseDurationMs(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "0" {
		return 0, nil
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ttl must be milliseconds")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

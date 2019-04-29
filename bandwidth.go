package bandwidth

import (
	"context"
	"fmt"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
)

const (
	interval = time.Second
)

type blkRecv struct {
	cid  cid.Cid
	from peer.ID
	size int
	last time.Duration
}

type blksSend struct {
	from peer.ID
	cids int
}

// BandWidth count peer's bandwidth
type BandWidth struct {
	ctx        context.Context
	incoming   chan blkRecv
	sendcoming chan blksSend

	data map[peer.ID]int // receive data size

	tdata map[peer.ID]time.Time // start timestamp

	cdata map[peer.ID]int // cids count

	sLck  sync.Mutex      // lock for sdata
	sdata map[peer.ID]int // bandwidth for peers
	tick  *time.Timer
}

// New returns an ptr of BandWidth
func New(ctx context.Context) *BandWidth {
	s := &BandWidth{
		ctx:        ctx,
		incoming:   make(chan blkRecv),
		sendcoming: make(chan blksSend),
		data:       make(map[peer.ID]int),
		tdata:      make(map[peer.ID]time.Time),
		cdata:      make(map[peer.ID]int),
		sdata:      make(map[peer.ID]int),
	}
	return s
}

// Start Bandwidth module
func (bd *BandWidth) Start() {
	bd.tick = time.NewTimer(5 * interval)
	for {
		select {
		case blk := <-bd.incoming:
			bd.handleIncoming(blk)
		case blks := <-bd.sendcoming:
			bd.handleSendcoming(blks)
		case <-bd.tick.C:
			bd.handleTick()
		case <-bd.ctx.Done():
			bd.tick.Stop()
			return
		}
	}
}

// ReceiveBlock called by blocks in
func (bd *BandWidth) ReceiveBlock(from peer.ID, size int, last time.Duration) {
	bd.incoming <- blkRecv{from: from, size: size, last: last}
}

// SendBlocks start bandwidth detect.
func (bd *BandWidth) SendBlocks(from peer.ID, blocks int) {
	bd.sendcoming <- blksSend{from: from, cids: blocks}
}

func (bd *BandWidth) handleTick() {

}

func (bd *BandWidth) handleIncoming(blk blkRecv) {
	bd.cdata[blk.from]--
	bd.data[blk.from] += blk.size

	if bd.cdata[blk.from] == 0 {
		start, ok := bd.tdata[blk.from]
		if ok {
			elaspe := time.Since(start).Seconds()
			speed := float64(bd.data[blk.from]) / elaspe
			bd.sLck.Lock()
			bd.sLck.Unlock()
			bandw, ok := bd.sdata[blk.from]
			if ok {
				bd.sdata[blk.from] = (bandw + int(speed/1024)) / 2
			} else {
				bd.sdata[blk.from] = int(speed / 1024)
			}
			fmt.Println("Bandwidth", blk.from.Pretty(), bd.sdata[blk.from], "kB/s")
		}
		bd.data[blk.from] = 0
	}
}

func (bd *BandWidth) handleSendcoming(blk blksSend) {
	bd.tdata[blk.from] = time.Now()
	bd.cdata[blk.from] += blk.cids
	bd.data[blk.from] = 0
}

// Has return whether the peer is tranfering data.
func (bd *BandWidth) Has(p peer.ID) bool {
	_, ok := bd.cdata[p]
	return ok
}

// Split split cids up to bandwidth.
func (bd *BandWidth) Split(ps []peer.ID, ks []cid.Cid) map[peer.ID][]cid.Cid {
	bd.sLck.Lock()
	bd.sLck.Unlock()
	values := make(map[peer.ID][]cid.Cid)
	getPer := func(val int) float32 {
		sum := 0
		for _, v := range bd.sdata {
			sum += v
		}
		if sum == 0 {
			return 0
		}
		return float32(val) / float32(sum)
	}

	checkZero := func(ps []peer.ID) bool {
		if len(bd.sdata) == 0 {
			return false
		}
		for _, p := range ps {
			_, ok := bd.sdata[p]
			if !ok {
				return false
			}
		}
		return true
	}
	if len(ps) == 0 {
		return values
	}
	if !checkZero(ps) {
		splits := make([][]cid.Cid, len(ps))
		for i, c := range ks {
			pos := i % len(ps)
			splits[pos] = append(splits[pos], c)
		}
		index := 0
		for _, v := range ps {
			values[v] = splits[index]
			index++
		}
	} else {
		start := 0
		var index peer.ID
		for _, v := range ps {
			index = v
			if start >= len(ks) {
				break
			}
			count := getPer(bd.sdata[v]) * float32(len(ks))
			if count == 0 {
				return values
			}
			end := start + int(count)
			fmt.Println(len(ks), v.Pretty(), start, end)
			if end > len(ks) {
				values[v] = ks[start:]
				break
			}
			values[v] = ks[start:end]
			start = end
		}
		if start < len(ks) {
			values[index] = append(values[index], ks[start:]...)
		}
	}

	return values
}

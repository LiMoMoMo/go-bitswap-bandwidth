package bandwidth

import (
	"context"
	"fmt"
	"time"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
)

const (
	interval = time.Second
)

type message interface {
	handle(bd *BandWidth)
}
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

type exitCheck struct {
	peerID peer.ID
	resp   chan bool
}

type PartialRequest struct {
	Peers []peer.ID
	Keys  []cid.Cid
}

type splitRequestMessage struct {
	ps   []peer.ID
	ks   []cid.Cid
	resp chan []*PartialRequest
}

// BandWidth count peer's bandwidth
type BandWidth struct {
	ctx      context.Context
	incoming chan message

	data map[peer.ID]int // receive data size

	tdata map[peer.ID]time.Time // start timestamp

	cdata map[peer.ID]int // cids count

	sdata map[peer.ID]int // bandwidth for peers
}

// New returns an ptr of BandWidth
func New(ctx context.Context) *BandWidth {
	s := &BandWidth{
		ctx:      ctx,
		incoming: make(chan message),
		data:     make(map[peer.ID]int),
		tdata:    make(map[peer.ID]time.Time),
		cdata:    make(map[peer.ID]int),
		sdata:    make(map[peer.ID]int),
	}
	return s
}

// Start Bandwidth module
func (bd *BandWidth) Start() {
	for {
		select {
		case msg := <-bd.incoming:
			msg.handle(bd)
		case <-bd.ctx.Done():
			return
		}
	}
}

// ReceiveBlock called by blocks in
func (bd *BandWidth) ReceiveBlock(from peer.ID, size int, last time.Duration) {
	bd.incoming <- &blkRecv{from: from, size: size, last: last}
}

// SendBlocks start bandwidth detect.
func (bd *BandWidth) SendBlocks(from peer.ID, blocks int) {
	bd.incoming <- &blksSend{from: from, cids: blocks}
}

// Has return whether the peer is tranfering data.
func (bd *BandWidth) Has(p peer.ID) bool {
	response := make(chan bool, 1)

	select {
	case bd.incoming <- &exitCheck{resp: response, peerID: p}:
	case <-bd.ctx.Done():
		return false
	}
	select {
	case resp := <-response:
		return resp
	case <-bd.ctx.Done():
		return false
	}
}

// Split split cids up to bandwidth.
func (bd *BandWidth) Split(ps []peer.ID, ks []cid.Cid) []*PartialRequest {
	resp := make(chan []*PartialRequest, 1)

	select {
	case bd.incoming <- &splitRequestMessage{ps, ks, resp}:
	case <-bd.ctx.Done():
		return nil
	}
	select {
	case splitRequests := <-resp:
		return splitRequests
	case <-bd.ctx.Done():
		return nil
	}
}

func (blk *blkRecv) handle(bd *BandWidth) {
	bd.cdata[blk.from]--
	bd.data[blk.from] += blk.size

	if bd.cdata[blk.from] == 0 {
		start, ok := bd.tdata[blk.from]
		if ok {
			elaspe := time.Since(start).Seconds()
			speed := float64(bd.data[blk.from]) / elaspe
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

func (blks *blksSend) handle(bd *BandWidth) {
	bd.tdata[blks.from] = time.Now()
	bd.cdata[blks.from] += blks.cids
	bd.data[blks.from] = 0
}

func (has *exitCheck) handle(bd *BandWidth) {
	_, ok := bd.cdata[has.peerID]
	has.resp <- ok
}

func (s *splitRequestMessage) handle(bd *BandWidth) {
	splitRequests := make([]*PartialRequest, len(s.ps))
	getPercent := func(val int) float32 {
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
	if len(s.ps) == 0 {
		s.resp <- splitRequests
		return
	}
	if !checkZero(s.ps) {
		splits := make([][]cid.Cid, len(s.ps))
		for i, c := range s.ks {
			pos := i % len(s.ps)
			splits[pos] = append(splits[pos], c)
		}
		index := 0
		for _, v := range s.ps {
			splitRequests[index] = &PartialRequest{[]peer.ID{v}, splits[index]}
			index++
		}
	} else {
		start := 0
		index := 0
		for _, v := range s.ps {

			if start >= len(s.ks) {
				break
			}
			count := getPercent(bd.sdata[v]) * float32(len(s.ks))
			if count == 0 {
				s.resp <- splitRequests
			}
			end := start + int(count)
			fmt.Println(len(s.ks), v.Pretty(), start, end)
			if end > len(s.ks) {
				splitRequests[index] = &PartialRequest{[]peer.ID{v}, s.ks[start:]}
				break
			}
			splitRequests[index] = &PartialRequest{[]peer.ID{v}, s.ks[start:end]}
			start = end
			index++
		}
		if start < len(s.ks) {
			splitRequests[index].Keys = append(splitRequests[index].Keys, s.ks[start:]...)
		}
	}

	s.resp <- splitRequests
}

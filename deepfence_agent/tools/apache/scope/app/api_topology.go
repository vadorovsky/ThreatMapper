package app

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"context"

	"github.com/gorilla/mux"
	ot "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/ugorji/go/codec"
	"github.com/weaveworks/scope/common/xfer"
	"github.com/weaveworks/scope/render"
	"github.com/weaveworks/scope/render/detailed"
	"github.com/weaveworks/scope/report"
)

const (
	websocketLoop = 1 * time.Second
)

// APITopology is returned by the /api/topology/{name} handler.
type APITopology struct {
	Nodes detailed.NodeSummaries `json:"nodes"`
}

// APINode is returned by the /api/topology/{name}/{id} handler.
type APINode struct {
	Node detailed.Node `json:"node"`
}

// RenderContextForReporter creates the rendering context for the given reporter.
func RenderContextForReporter(rep Reporter, r report.Report) detailed.RenderContext {
	rc := detailed.RenderContext{Report: r}
	if wrep, ok := rep.(WebReporter); ok {
		rc.MetricsGraphURL = wrep.MetricsGraphURL
	}
	return rc
}

type rendererHandler func(context.Context, render.Renderer, render.Transformer, detailed.RenderContext, http.ResponseWriter, *http.Request)

// Full topology.
func handleTopology(ctx context.Context, renderer render.Renderer, transformer render.Transformer, rc detailed.RenderContext, w http.ResponseWriter, r *http.Request) {
	censorCfg := report.GetCensorConfigFromRequest(r)
	nodeSummaries := detailed.Summaries(ctx, rc, render.Render(ctx, rc.Report, renderer, transformer).Nodes, true)

	respondWith(ctx, w, http.StatusOK, APITopology{
		Nodes: detailed.CensorNodeSummaries(nodeSummaries, censorCfg),
	})
}

// WriteToFile writes a Report to a file. The encoding is determined
// by the file extension (".msgpack" or ".json", with an optional
// ".gz").
func WriteToFile(path string, rep APINode) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	msgpack, gzipped, err := fileType(path)
	if err != nil {
		return err
	}

	var w io.Writer
	bufwriter := bufio.NewWriter(f)
	defer bufwriter.Flush()
	w = bufwriter
	if gzipped {
		gzwriter := gzipWriterPool.Get().(*gzip.Writer)
		gzwriter.Reset(w)
		defer gzipWriterPool.Put(gzwriter)
		defer gzwriter.Close()
		w = gzwriter
	}

	return codec.NewEncoder(w, codecHandle(msgpack)).Encode(rep)
}
func codecHandle(msgpack int) codec.Handle {
	if (msgpack == 0) {
		return &codec.JsonHandle{}
	} else if (msgpack == 1) {
		return &codec.MsgpackHandle{}
	} else if (msgpack == 2) {
		return &codec.BincHandle{}
	}
	return nil
}
var gzipWriterPool = &sync.Pool{
	// NewWriterLevel() only errors if the compression level is invalid, which can't happen here
	New: func() interface{} { w, _ := gzip.NewWriterLevel(nil, gzip.DefaultCompression); return w },
}

func fileType(path string) (msgpack int, gzipped bool, err error) {
	fileType := filepath.Ext(path)
	gzipped = false
	if fileType == ".gz" {
		gzipped = true
		fileType = filepath.Ext(strings.TrimSuffix(path, fileType))
	}
	switch fileType {
	case ".json":
		return 0, gzipped, nil
	case ".msgpack":
		return 1, gzipped, nil
	case ".binc":
		return 2, gzipped, nil
	default:
		return 3, false, fmt.Errorf("Unsupported file extension: %v", fileType)
	}
}

// Individual nodes.
func handleNode(ctx context.Context, renderer render.Renderer, transformer render.Transformer, rc detailed.RenderContext, w http.ResponseWriter, r *http.Request) {
	var (
		censorCfg  = report.GetCensorConfigFromRequest(r)
		vars       = mux.Vars(r)
		topologyID = vars["topology"]
		nodeID     = vars["id"]
	)
	// We must not lose the node during filtering. We achieve that by
	// (1) rendering the report with the base renderer, without
	// filtering, which gives us the node (if it exists at all), and
	// then (2) applying the filter separately to that result.  If the
	// node is lost in the second step, we simply put it back.
	nodes := renderer.Render(ctx, rc.Report)
	node, ok := nodes.Nodes[nodeID]
	if !ok {
		http.NotFound(w, r)
		return
	}
	nodes = transformer.Transform(nodes)
	if filteredNode, ok := nodes.Nodes[nodeID]; ok {
		node = filteredNode
	} else { // we've lost the node during filtering; put it back
		nodes.Nodes[nodeID] = node
		fmt.Println("Nodes.Filtered: "+strconv.Itoa(nodes.Filtered))
		nodes.Filtered--
	}
	rawNode := detailed.MakeNode(topologyID, rc, nodes.Nodes, node)
	respondWith(ctx, w, http.StatusOK, APINode{Node: detailed.CensorNode(rawNode, censorCfg)})
	fmt.Println("Responding /topology/hosts: "+"var/log/response.json")
	WriteToFile("var/log/response.json", APINode{Node: detailed.CensorNode(rawNode, censorCfg)})
}

// Websocket for the full topology.
func handleWebsocket(
	ctx context.Context,
	rep Reporter,
	w http.ResponseWriter,
	r *http.Request,
) {
	if err := r.ParseForm(); err != nil {
		respondWith(ctx, w, http.StatusInternalServerError, err)
		return
	}
	loop := websocketLoop
	if t := r.Form.Get("t"); t != "" {
		var err error
		if loop, err = time.ParseDuration(t); err != nil {
			respondWith(ctx, w, http.StatusBadRequest, t)
			return
		}
	}

	conn, err := xfer.Upgrade(w, r, nil)
	if err != nil {
		// log.Info("Upgrade:", err)
		return
	}
	defer conn.Close()

	quit := make(chan struct{})
	go func(c xfer.Websocket) {
		for { // just discard everything the browser sends
			if _, _, err := c.ReadMessage(); err != nil {
				if !xfer.IsExpectedWSCloseError(err) {
					log.Error("err:", err)
				}
				close(quit)
				break
			}
		}
	}(conn)

	wc := websocketState{
		rep:              rep,
		values:           r.Form,
		conn:             conn,
		topologyID:       mux.Vars(r)["topology"],
		startReportingAt: deserializeTimestamp(r.Form.Get("timestamp")),
		censorCfg:        report.GetCensorConfigFromRequest(r),
		channelOpenedAt:  time.Now(),
	}
	adjacencyStr := r.Form.Get("adjacency")
	if adjacencyStr == "false" {
		wc.adjacency = false
	} else {
		wc.adjacency = true
	}

	wait := make(chan struct{}, 1)
	rep.WaitOn(ctx, wait)
	defer rep.UnWait(ctx, wait)

	tick := time.Tick(loop)
	for {
		if err := wc.update(ctx); err != nil {
			log.Errorf("%v", err)
			return
		}

		select {
		case <-wait:
		case <-tick:
		case <-quit:
			return
		}
	}
}

type websocketState struct {
	rep              Reporter
	values           url.Values
	conn             xfer.Websocket
	previousTopo     detailed.NodeSummaries
	topologyID       string
	startReportingAt time.Time
	reportTimestamp  time.Time
	censorCfg        report.CensorConfig
	channelOpenedAt  time.Time
	adjacency        bool
}

func (wc *websocketState) update(ctx context.Context) error {
	span := ot.StartSpan("websocket.Render", ot.Tag{"topology", wc.topologyID})
	defer span.Finish()
	ctx = ot.ContextWithSpan(ctx, span)
	// We measure how much time has passed since the channel was opened
	// and add it to the initial report timestamp to get the timestamp
	// of the snapshot we want to report right now.
	// NOTE: Multiplying `timestampDelta` by a constant factor here
	// would have an effect of fast-forward, which is something we
	// might be interested in implementing in the future.
	timestampDelta := time.Since(wc.channelOpenedAt)
	reportTimestamp := wc.startReportingAt.Add(timestampDelta)
	span.LogFields(otlog.String("opened-at", wc.channelOpenedAt.String()),
		otlog.String("timestamp", reportTimestamp.String()))
	re, err := wc.rep.Report(ctx, reportTimestamp)
	if err != nil {
		return errors.Wrap(err, "Error generating report")
	}
	if wc.adjacency == false {
		re.Endpoint = report.MakeTopology()
	}
	renderer, filter, err := topologyRegistry.RendererForTopology(wc.topologyID, wc.values, re)
	if err != nil {
		return errors.Wrap(err, "Error generating report")
	}

	newTopo := detailed.CensorNodeSummaries(
		detailed.Summaries(
			ctx,
			RenderContextForReporter(wc.rep, re),
			render.Render(ctx, re, renderer, filter).Nodes,
			wc.adjacency,
		),
		wc.censorCfg,
	)
	diff := detailed.TopoDiff(wc.previousTopo, newTopo)
	wc.previousTopo = newTopo

	if err := wc.conn.WriteJSON(diff); err != nil {
		if !xfer.IsExpectedWSCloseError(err) {
			return errors.Wrap(err, "cannot serialize topology diff")
		}
	}
	return nil
}

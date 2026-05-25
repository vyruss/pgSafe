package rpc

// Dual-codec: one stdlib net/rpc connection, two on-the-wire encodings.
//
// The control plane (Hello, Configure, StreamFile, ...) speaks JSON for
// readability — small messages, easy to tcpdump/strace. The bulk plane
// (WriteChunk and any future binary-payload method) speaks gob so that
// `[]byte` payloads travel without base64 inflation or per-byte JSON
// parsing cost.
//
// Both formats ride one SSH stdio pair via a fixed envelope:
//
//	[1] disc       'J' = JSON body, 'G' = gob body
//	[8] seq        BE uint64; net/rpc Request/Response.Seq
//	[1] kind       'q' = request, 'r' = response
//	[4] mlen       BE uint32; length of method name (in bytes)
//	[*] method     ASCII method name (echoed in responses)
//	[4] elen       BE uint32; length of error message (response only;
//	               always 0 on a request)
//	[*] error      UTF-8 error string (empty when len=0)
//	[4] blen       BE uint32; length of body (encoded per disc)
//	[*] body       JSON or gob bytes per disc
//
// The server records the discriminator per request (so responses match)
// and the client picks the discriminator per Call (gob for methods named
// in GobMethods, JSON for everything else). Two ends of the same wire,
// one codec implementation each.

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net/rpc"
	"sync"
)

// Discriminator values. ASCII for grep-ability in hex dumps.
const (
	discJSON byte = 'J'
	discGob  byte = 'G'
)

const (
	kindRequest  byte = 'q'
	kindResponse byte = 'r'
)

// frame is the envelope every read or write moves through. body's encoding
// is determined by disc; the framing layer treats it as opaque bytes.
type frame struct {
	disc   byte
	seq    uint64
	kind   byte
	method string
	errMsg string // response side; empty on requests
	body   []byte
}

func readFrame(r io.Reader) (frame, error) {
	var f frame
	var disc [1]byte
	if _, err := io.ReadFull(r, disc[:]); err != nil {
		return f, err
	}
	f.disc = disc[0]
	if f.disc != discJSON && f.disc != discGob {
		return f, fmt.Errorf("dualcodec: bad discriminator %#x", f.disc)
	}

	if err := binary.Read(r, binary.BigEndian, &f.seq); err != nil {
		return f, err
	}

	var kind [1]byte
	if _, err := io.ReadFull(r, kind[:]); err != nil {
		return f, err
	}
	f.kind = kind[0]
	if f.kind != kindRequest && f.kind != kindResponse {
		return f, fmt.Errorf("dualcodec: bad kind %#x", f.kind)
	}

	method, err := readLenPrefixed(r)
	if err != nil {
		return f, err
	}
	f.method = string(method)

	errMsg, err := readLenPrefixed(r)
	if err != nil {
		return f, err
	}
	f.errMsg = string(errMsg)

	body, err := readLenPrefixed(r)
	if err != nil {
		return f, err
	}
	f.body = body

	return f, nil
}

func writeFrame(w io.Writer, f frame) error {
	// Compose the whole frame in a buffer first so the underlying writer
	// sees one contiguous Write — keeps interleaving with concurrent
	// writers safe when callers serialise externally.
	var buf bytes.Buffer
	buf.WriteByte(f.disc)
	if err := binary.Write(&buf, binary.BigEndian, f.seq); err != nil {
		return err
	}
	buf.WriteByte(f.kind)
	if err := writeLenPrefixed(&buf, []byte(f.method)); err != nil {
		return err
	}
	if err := writeLenPrefixed(&buf, []byte(f.errMsg)); err != nil {
		return err
	}
	if err := writeLenPrefixed(&buf, f.body); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}

func readLenPrefixed(r io.Reader) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]byte, n)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeLenPrefixed(w io.Writer, p []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(p))); err != nil { //nolint:gosec
		return err
	}
	if len(p) == 0 {
		return nil
	}
	_, err := w.Write(p)
	return err
}

// encodeBody marshals body per the chosen discriminator. JSON for 'J',
// gob for 'G'. nil body is encoded as the codec's empty value (zero-length
// JSON null / zero-length gob payload).
func encodeBody(disc byte, body any) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	switch disc {
	case discJSON:
		return json.Marshal(body)
	case discGob:
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return nil, fmt.Errorf("dualcodec: encode: bad disc %#x", disc)
}

// decodeBody unmarshals payload into body per disc. Empty payload is
// treated as "no body" — the call succeeds without touching body.
func decodeBody(disc byte, payload []byte, body any) error {
	if body == nil || len(payload) == 0 {
		return nil
	}
	switch disc {
	case discJSON:
		return json.Unmarshal(payload, body)
	case discGob:
		return gob.NewDecoder(bytes.NewReader(payload)).Decode(body)
	}
	return fmt.Errorf("dualcodec: decode: bad disc %#x", disc)
}

// ----- Server codec -----

type serverCodec struct {
	rwc     io.ReadWriteCloser
	writeMu sync.Mutex
	pending frame // last-read request; remembered through to WriteResponse
}

// NewDualServerCodec wraps an io.ReadWriteCloser as an rpc.ServerCodec
// that speaks the dual-codec envelope. Pass to rpc.Server.ServeCodec.
func NewDualServerCodec(rwc io.ReadWriteCloser) rpc.ServerCodec {
	return &serverCodec{rwc: rwc}
}

func (c *serverCodec) ReadRequestHeader(req *rpc.Request) error {
	f, err := readFrame(c.rwc)
	if err != nil {
		return err
	}
	if f.kind != kindRequest {
		return fmt.Errorf("dualcodec server: expected request, got kind %#x", f.kind)
	}
	c.pending = f
	req.ServiceMethod = f.method
	req.Seq = f.seq
	return nil
}

func (c *serverCodec) ReadRequestBody(body any) error {
	return decodeBody(c.pending.disc, c.pending.body, body)
}

func (c *serverCodec) WriteResponse(resp *rpc.Response, body any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	out := frame{
		disc:   c.pending.disc,
		seq:    resp.Seq,
		kind:   kindResponse,
		method: resp.ServiceMethod,
		errMsg: resp.Error,
	}
	if resp.Error == "" && body != nil {
		bs, err := encodeBody(c.pending.disc, body)
		if err != nil {
			return err
		}
		out.body = bs
	}
	return writeFrame(c.rwc, out)
}

func (c *serverCodec) Close() error {
	return c.rwc.Close()
}

// ----- Client codec -----

type clientCodec struct {
	rwc     io.ReadWriteCloser
	writeMu sync.Mutex

	// gobMethods is the set of fully-qualified method names that should
	// be sent using the gob discriminator. Everything else uses JSON.
	gobMethods map[string]struct{}

	// pendingDisc tracks the discriminator chosen for each in-flight Seq
	// so ReadResponseBody knows how to decode the response. net/rpc
	// guarantees Seq uniqueness per Client.
	pendingMu   sync.Mutex
	pendingDisc map[uint64]byte

	// lastFrame is filled by ReadResponseHeader and consumed by
	// ReadResponseBody — the read side is single-threaded by net/rpc, so
	// no lock is required between the two.
	lastFrame frame
}

// NewDualClientCodec wraps an io.ReadWriteCloser as an rpc.ClientCodec.
// Methods named in gobMethods send their request body in gob; all other
// methods use JSON. Pass to rpc.NewClientWithCodec.
func NewDualClientCodec(rwc io.ReadWriteCloser, gobMethods []string) rpc.ClientCodec {
	set := make(map[string]struct{}, len(gobMethods))
	for _, m := range gobMethods {
		set[m] = struct{}{}
	}
	return &clientCodec{
		rwc:         rwc,
		gobMethods:  set,
		pendingDisc: make(map[uint64]byte),
	}
}

func (c *clientCodec) WriteRequest(req *rpc.Request, body any) error {
	disc := discJSON
	if _, ok := c.gobMethods[req.ServiceMethod]; ok {
		disc = discGob
	}
	bs, err := encodeBody(disc, body)
	if err != nil {
		return err
	}
	c.pendingMu.Lock()
	c.pendingDisc[req.Seq] = disc
	c.pendingMu.Unlock()

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return writeFrame(c.rwc, frame{
		disc:   disc,
		seq:    req.Seq,
		kind:   kindRequest,
		method: req.ServiceMethod,
		body:   bs,
	})
}

func (c *clientCodec) ReadResponseHeader(resp *rpc.Response) error {
	f, err := readFrame(c.rwc)
	if err != nil {
		return err
	}
	if f.kind != kindResponse {
		return fmt.Errorf("dualcodec client: expected response, got kind %#x", f.kind)
	}
	c.lastFrame = f
	resp.ServiceMethod = f.method
	resp.Seq = f.seq
	resp.Error = f.errMsg
	return nil
}

func (c *clientCodec) ReadResponseBody(body any) error {
	// net/rpc uses lastFrame.seq to look up the in-flight Call. Pop the
	// recorded discriminator: this Seq will never come back.
	c.pendingMu.Lock()
	disc, ok := c.pendingDisc[c.lastFrame.seq]
	if ok {
		delete(c.pendingDisc, c.lastFrame.seq)
	} else {
		// Defensive: server-frame discriminator is authoritative if we
		// somehow lost the request side (shouldn't happen — net/rpc
		// guarantees one WriteRequest per Seq).
		disc = c.lastFrame.disc
	}
	c.pendingMu.Unlock()
	if c.lastFrame.errMsg != "" {
		// rpc.Client surfaces the error via resp.Error already; the body
		// is undefined when an error is present.
		return nil
	}
	return decodeBody(disc, c.lastFrame.body, body)
}

func (c *clientCodec) Close() error {
	return c.rwc.Close()
}

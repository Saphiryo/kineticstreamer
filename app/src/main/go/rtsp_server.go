package kinetic

import (
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/auth"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/bluenviron/gortsplib/v4/pkg/headers"
)

type RTSPServerSink struct {
	disk   *BinaryDumpSink
	s      *gortsplib.Server
	stream *gortsplib.ServerStream

	recordedPlaybackRequestId atomic.Uint32

	// done is closed by Close to stop playback goroutines, which wg tracks.
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	// mu guards pts0 and the tracks' seq, which are shared between live writes and playback.
	mu     sync.Mutex
	tracks []*rtspTrack
	pts0   int64

	// Optional RTSP authentication (Basic or Digest). Disabled when both are empty.
	user, pass string
	nonce      string
}

const rtspAuthRealm = "KineticStreamer"

// checkAuth returns a 401 response when credentials are required and missing or wrong.
// baseURLs are alternative digest URIs accepted besides the request URL.
func (sh *RTSPServerSink) checkAuth(req *base.Request, baseURLs ...*base.URL) *base.Response {
	if sh.user == "" && sh.pass == "" {
		return nil
	}
	if len(baseURLs) == 0 {
		baseURLs = []*base.URL{nil}
	}
	var err error
	for _, u := range baseURLs {
		if err = auth.Validate(req, sh.user, sh.pass, u, nil, rtspAuthRealm, sh.nonce); err == nil {
			return nil
		}
	}
	log.Printf("rtsp auth failed: %v", err)
	return &base.Response{
		StatusCode: base.StatusUnauthorized,
		Header: base.Header{
			"WWW-Authenticate": auth.GenerateWWWAuthenticate(nil, rtspAuthRealm, sh.nonce),
		},
	}
}

// setupBaseURLs are the request URL without its last path element (the track control),
// with and without a trailing slash: VLC/live555 uses the session (Content-Base) URL as
// the digest URI of SETUP requests.
func setupBaseURLs(req *base.Request) []*base.URL {
	if req.URL == nil {
		return nil
	}
	u := *req.URL
	u.RawQuery = ""
	if i := strings.LastIndex(u.Path, "/"); i > 0 {
		u.Path = u.Path[:i]
	}
	withSlash := u
	withSlash.Path += "/"
	return []*base.URL{&u, &withSlash}
}

type rtspTrack struct {
	media *description.Media

	mtu, seq  uint16
	payloader rtp.Payloader

	// H.264/H.265 parameter sets (MediaCodec codec-config buffer), resent with every
	// keyframe so clients joining mid-stream can decode.
	isH26x      bool
	codecConfig []byte
}

// called when a connection is opened.
func (sh *RTSPServerSink) OnConnOpen(ctx *gortsplib.ServerHandlerOnConnOpenCtx) {
	log.Printf("conn opened")
}

// called when a connection is closed.
func (sh *RTSPServerSink) OnConnClose(ctx *gortsplib.ServerHandlerOnConnCloseCtx) {
	log.Printf("conn closed (%v)", ctx.Error)
}

// called when a session is opened.
func (sh *RTSPServerSink) OnSessionOpen(ctx *gortsplib.ServerHandlerOnSessionOpenCtx) {
	log.Printf("session opened")
}

// called when a session is closed.
func (sh *RTSPServerSink) OnSessionClose(ctx *gortsplib.ServerHandlerOnSessionCloseCtx) {
	log.Printf("session closed")
}

// called when receiving a DESCRIBE request.
func (sh *RTSPServerSink) OnDescribe(ctx *gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	log.Printf("describe request")
	if res := sh.checkAuth(ctx.Request); res != nil {
		return res, nil, nil
	}

	return &base.Response{
		StatusCode: base.StatusOK,
	}, sh.stream, nil
}

// called when receiving an ANNOUNCE request.
func (sh *RTSPServerSink) OnAnnounce(ctx *gortsplib.ServerHandlerOnAnnounceCtx) (*base.Response, error) {
	log.Printf("announce request")

	// this server doesn't accept publishes from another client
	return &base.Response{
		StatusCode: base.StatusForbidden,
	}, nil
}

// called when receiving a SETUP request.
func (sh *RTSPServerSink) OnSetup(ctx *gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	log.Printf("setup request")
	if res := sh.checkAuth(ctx.Request, setupBaseURLs(ctx.Request)...); res != nil {
		return res, nil, nil
	}

	return &base.Response{
		StatusCode: base.StatusOK,
	}, sh.stream, nil
}

// called when receiving a PLAY request.
func (sh *RTSPServerSink) OnPlay(ctx *gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	log.Printf("play request")
	if res := sh.checkAuth(ctx.Request, setupBaseURLs(ctx.Request)...); res != nil {
		return res, nil
	}

	// parse the range header
	rangeHeader := ctx.Request.Header["Range"]
	if len(rangeHeader) == 0 {
		sh.recordedPlaybackRequestId.Store(0)
		return &base.Response{
			StatusCode: base.StatusOK,
		}, nil
	}

	h := &headers.Range{}
	if err := h.Unmarshal(rangeHeader); err != nil {
		return &base.Response{
			StatusCode: base.StatusBadRequest,
		}, err
	}
	var dpts int64
	switch value := h.Value.(type) {
	case *headers.RangeSMPTE:
		// TODO: implement
		return &base.Response{StatusCode: base.StatusOK}, nil
	case *headers.RangeNPT:
		log.Printf("value %#v", value)
		dpts = value.Start.Microseconds()
	case *headers.RangeUTC:
		// TODO: implement
		return &base.Response{StatusCode: base.StatusOK}, nil
	}
	if dpts == 0 || sh.disk == nil {
		// exit if the range header indicates that we should play live.
		sh.recordedPlaybackRequestId.Store(0)
		return &base.Response{
			StatusCode: base.StatusOK,
		}, nil
	}
	sh.mu.Lock()
	requestedPTS := sh.pts0 + dpts
	sh.mu.Unlock()
	log.Printf("reading from %d", requestedPTS)
	sr, err := sh.disk.SampleReader(requestedPTS)
	if err != nil {
		return &base.Response{StatusCode: base.StatusInternalServerError}, err
	}
	requestId := sh.recordedPlaybackRequestId.Add(1)
	sh.wg.Add(1)
	go func() {
		defer sh.wg.Done()
		defer sr.Close()
		// resume live streaming once the recording has been played out.
		defer sh.recordedPlaybackRequestId.CompareAndSwap(requestId, 0)
		start := time.Now()
		var first int64
		havePTS0 := false
		for sh.recordedPlaybackRequestId.Load() == requestId {
			sample, err := sr.Next()
			if err != nil {
				return
			}
			if sample.Track >= len(sh.tracks) {
				continue
			}
			// pace the samples at the rate they were recorded.
			if !havePTS0 {
				first, havePTS0 = sample.PTS, true
			}
			if wait := time.Duration(sample.PTS-first)*time.Microsecond - time.Since(start); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-sh.done:
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			if sh.recordedPlaybackRequestId.Load() != requestId {
				return
			}
			if err := sh.writePacket(sample.Track, sample.Data, sample.PTS); err != nil {
				return
			}
		}
	}()
	return &base.Response{
		StatusCode: base.StatusOK,
	}, nil
}

// called when receiving a PAUSE request.
func (sh *RTSPServerSink) OnPause(ctx *gortsplib.ServerHandlerOnPauseCtx) (*base.Response, error) {
	log.Printf("pause request %#v", ctx)

	return &base.Response{
		StatusCode: base.StatusOK,
	}, nil
}

func (sh *RTSPServerSink) OnGetParameter(ctx *gortsplib.ServerHandlerOnGetParameterCtx) (*base.Response, error) {
	log.Printf("get parameter request %#v", ctx)

	return &base.Response{
		StatusCode: base.StatusOK,
	}, nil
}

func (sh *RTSPServerSink) OnSetParameter(ctx *gortsplib.ServerHandlerOnSetParameterCtx) (*base.Response, error) {
	log.Printf("set parameter request %#v", ctx)

	return &base.Response{
		StatusCode: base.StatusOK,
	}, nil
}

func (sh *RTSPServerSink) OnPacketLost(ctx *gortsplib.ServerHandlerOnPacketLostCtx) {
	log.Printf("packet lost")
}

func (sh *RTSPServerSink) OnDecodeError(ctx *gortsplib.ServerHandlerOnDecodeErrorCtx) {
	log.Printf("decode error")
}

func (sh *RTSPServerSink) OnStreamWriteError(ctx *gortsplib.ServerHandlerOnStreamWriteErrorCtx) {
	log.Printf("stream write error")
}

// called when receiving a RECORD request.
func (sh *RTSPServerSink) OnRecord(ctx *gortsplib.ServerHandlerOnRecordCtx) (*base.Response, error) {
	log.Printf("record request")

	return &base.Response{
		StatusCode: base.StatusForbidden,
	}, nil
}

func toMediaAndPayloader(mediaFormatMimeType string, pt uint8) (*description.Media, rtp.Payloader) {
	mimeType := MediaFormatMimeType(mediaFormatMimeType).PionMimeType()
	switch mimeType {
	case webrtc.MimeTypeH264:
		return &description.Media{
			Type: description.MediaTypeVideo,
			Formats: []format.Format{&format.H264{
				PayloadTyp:        pt,
				PacketizationMode: 1,
			}},
		}, &codecs.H264Payloader{}
	case webrtc.MimeTypeVP8:
		return &description.Media{
			Type: description.MediaTypeVideo,
			Formats: []format.Format{&format.VP8{
				PayloadTyp: pt,
			}},
		}, &codecs.VP8Payloader{}
	case webrtc.MimeTypeVP9:
		return &description.Media{
			Type: description.MediaTypeVideo,
			Formats: []format.Format{&format.VP9{
				PayloadTyp: pt,
			}},
		}, &codecs.VP9Payloader{}
	case webrtc.MimeTypeAV1:
		return &description.Media{
			Type: description.MediaTypeVideo,
			Formats: []format.Format{&format.AV1{
				PayloadTyp: pt,
			}},
		}, &codecs.AV1Payloader{}
	case webrtc.MimeTypeOpus:
		return &description.Media{
			Type: description.MediaTypeAudio,
			Formats: []format.Format{&format.Opus{
				PayloadTyp: pt,
			}},
		}, &codecs.OpusPayloader{}
	default:
		return nil, nil
	}
}

func NewRTSPServerSink(disk *BinaryDumpSink, encodedMediaFormatMimeTypes string) (*RTSPServerSink, error) {
	return newRTSPServerSink(disk, encodedMediaFormatMimeTypes, ":8554", ":8000", ":8001")
}

// NewRTSPServerSinkWithAuth starts an RTSP server on [port] (e.g. "8554"). When user or pass
// is non-empty, clients must authenticate (Basic or Digest). disk may be nil (no seeking).
func NewRTSPServerSinkWithAuth(disk *BinaryDumpSink, encodedMediaFormatMimeTypes, port, user, pass string) (*RTSPServerSink, error) {
	if port == "" {
		port = "8554"
	}
	return newRTSPServerSinkAuth(disk, encodedMediaFormatMimeTypes, ":"+port, ":8000", ":8001", user, pass)
}

func newRTSPServerSink(disk *BinaryDumpSink, encodedMediaFormatMimeTypes, rtspAddress, udpRTPAddress, udpRTCPAddress string) (_ *RTSPServerSink, err error) {
	return newRTSPServerSinkAuth(disk, encodedMediaFormatMimeTypes, rtspAddress, udpRTPAddress, udpRTCPAddress, "", "")
}

func newRTSPServerSinkAuth(disk *BinaryDumpSink, encodedMediaFormatMimeTypes, rtspAddress, udpRTPAddress, udpRTCPAddress, user, pass string) (_ *RTSPServerSink, err error) {
	log.Printf("encodedMediaFormatMimeTypes: %s", encodedMediaFormatMimeTypes)
	mediaFormatMimeTypes := strings.Split(encodedMediaFormatMimeTypes, ";")
	nonce, err := auth.GenerateNonce()
	if err != nil {
		return nil, err
	}
	s := &RTSPServerSink{
		disk:   disk,
		done:   make(chan struct{}),
		tracks: make([]*rtspTrack, len(mediaFormatMimeTypes)),
		user:   user,
		pass:   pass,
		nonce:  nonce,
	}
	s.s = &gortsplib.Server{
		Handler:           s,
		RTSPAddress:       rtspAddress,
		UDPRTPAddress:     udpRTPAddress,
		UDPRTCPAddress:    udpRTCPAddress,
		MulticastIPRange:  "224.1.0.0/16",
		MulticastRTPPort:  8002,
		MulticastRTCPPort: 8003,
	}

	// the server must be started first to pick up all the default values.
	if err := s.s.Start(); err != nil {
		return nil, err
	}
	// release the ports if anything below fails.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
		if err != nil {
			s.s.Close()
		}
	}()

	medias := make([]*description.Media, len(mediaFormatMimeTypes))
	for i, mediaFormatMimeType := range mediaFormatMimeTypes {
		media, payloader := toMediaAndPayloader(mediaFormatMimeType, uint8(i+96))
		if media == nil || payloader == nil {
			return nil, fmt.Errorf("invalid media format mime type: %s", mediaFormatMimeType)
		}
		pionMime := MediaFormatMimeType(mediaFormatMimeType).PionMimeType()
		s.tracks[i] = &rtspTrack{
			media:     media,
			payloader: payloader,
			mtu:       uint16(s.s.MaxPacketSize),
			isH26x:    pionMime == webrtc.MimeTypeH264 || pionMime == webrtc.MimeTypeH265,
		}
		medias[i] = media
		log.Printf("media %d: %+v", i, media)
	}

	s.stream = gortsplib.NewServerStream(s.s, &description.Session{Medias: medias})

	return s, nil
}

// writePacket packetizes buf and sends it to the clients.
func (s *RTSPServerSink) writePacket(i int, buf []byte, ptsMicroseconds int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tracks[i]

	pts := time.Duration(ptsMicroseconds) * time.Microsecond
	ts := uint32(pts.Seconds() * float64(t.media.Formats[0].ClockRate()))

	payloads := t.payloader.Payload(t.mtu-12, buf)

	for i, pp := range payloads {
		if err := s.stream.WritePacketRTP(t.media, &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				Marker:         i == len(payloads)-1,
				PayloadType:    t.media.Formats[0].PayloadType(),
				SequenceNumber: t.seq,
				Timestamp:      ts,
			},
			Payload: pp,
		}); err != nil {
			return err
		}
		t.seq++
	}
	return nil
}

// MediaCodec buffer flags
const (
	mediaCodecFlagKeyFrame    = 1
	mediaCodecFlagCodecConfig = 2
)

// WriteSampleWithFlags is WriteSample with MediaCodec buffer flags: codec-config buffers are
// kept and prepended to every keyframe instead of being sent once.
func (s *RTSPServerSink) WriteSampleWithFlags(i int, buf []byte, ptsMicroseconds int64, mediaCodecFlags int32) error {
	if i < 0 || i >= len(s.tracks) {
		return fmt.Errorf("track index out of range: %d", i)
	}
	t := s.tracks[i]
	if t.isH26x {
		if mediaCodecFlags&mediaCodecFlagCodecConfig != 0 {
			s.mu.Lock()
			t.codecConfig = append([]byte(nil), buf...)
			s.mu.Unlock()
			return nil
		}
		if mediaCodecFlags&mediaCodecFlagKeyFrame != 0 {
			s.mu.Lock()
			cfg := t.codecConfig
			s.mu.Unlock()
			if len(cfg) > 0 {
				joined := make([]byte, 0, len(cfg)+len(buf))
				joined = append(joined, cfg...)
				buf = append(joined, buf...)
			}
		}
	}
	return s.WriteSample(i, buf, ptsMicroseconds)
}

func (s *RTSPServerSink) WriteSample(i int, buf []byte, ptsMicroseconds int64) error {
	if s.recordedPlaybackRequestId.Load() > 0 {
		return nil
	}
	if i < 0 || i >= len(s.tracks) {
		return fmt.Errorf("track index out of range: %d", i)
	}
	s.mu.Lock()
	if s.pts0 == 0 {
		s.pts0 = ptsMicroseconds
	}
	s.mu.Unlock()
	return s.writePacket(i, buf, ptsMicroseconds)
}

// Close stops playback and releases the RTSP ports.
func (s *RTSPServerSink) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
		s.stream.Close()
		s.s.Close()
		s.wg.Wait()
	})
	return nil
}

// Package surreallog implements a CoreDNS plugin that logs DNS queries and responses
// to SurrealDB with async batched writes. It captures full query/response data
// without blocking DNS resolution.
package surreallog

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
)

// SurrealLog is the plugin handler.
type SurrealLog struct {
	Next   plugin.Handler
	writer *Writer
	config *Config
}

// LogEntry holds a single DNS query/response pair for async writing.
type LogEntry struct {
	Timestamp    time.Time
	Server       string
	ClientIP     string
	ClientPort   int
	QName        string
	QType        string
	QClass       string
	Protocol     string
	QuerySize    int
	RCode        string
	AnswerCount  int
	ResponseSize int
	LatencyUs    int64
	Flags        string
	Answers      []AnswerRecord
	RawQuery     []byte
	RawResponse  []byte
}

// AnswerRecord is a parsed answer from the response.
type AnswerRecord struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Content string `json:"content"`
	TTL     int    `json:"ttl"`
}

// ServeDNS implements the plugin.Handler interface.
func (s *SurrealLog) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	start := time.Now()

	// Wrap the response writer to capture the response
	recorder := dnstest.NewRecorder(w)

	// Pass to next plugin — this is where the actual DNS answer is generated
	rcode, err := plugin.NextOrFailure(s.Name(), s.Next, ctx, recorder, r)

	// Build the log entry from the captured query and response
	entry := s.buildEntry(start, w, r, recorder, rcode)

	// Send to async writer (non-blocking)
	select {
	case s.writer.logChan <- entry:
		loggedTotal.Inc()
	default:
		// Channel full — drop the log entry, never block DNS
		droppedTotal.Inc()
	}

	return rcode, err
}

// Name implements the plugin.Handler interface.
func (s *SurrealLog) Name() string { return "surreallog" }

// buildEntry constructs a LogEntry from the query/response pair.
func (s *SurrealLog) buildEntry(start time.Time, w dns.ResponseWriter, query *dns.Msg, recorder *dnstest.Recorder, rcode int) LogEntry {
	state := request.Request{W: w, Req: query}

	entry := LogEntry{
		Timestamp: start.UTC(),
		Server:    s.config.ServerID,
		QName:     state.QName(),
		QType:     state.Type(),
		QClass:    state.Class(),
		Protocol:  state.Proto(),
		QuerySize: query.Len(),
		LatencyUs: time.Since(start).Microseconds(),
	}

	// Client IP and port
	host, port, err := net.SplitHostPort(w.RemoteAddr().String())
	if err == nil {
		entry.ClientIP = host
		p := 0
		for _, c := range port {
			p = p*10 + int(c-'0')
		}
		entry.ClientPort = p
	}

	// Response data
	if recorder.Msg != nil {
		entry.RCode = dns.RcodeToString[recorder.Msg.Rcode]
		entry.AnswerCount = len(recorder.Msg.Answer)
		entry.ResponseSize = recorder.Msg.Len()
		entry.Flags = buildFlags(recorder.Msg)

		// Parse answer records
		if s.config.LogAnswers {
			entry.Answers = parseAnswers(recorder.Msg.Answer)
		}

		// Raw packets (if configured)
		if s.config.LogRaw {
			if packed, err := query.Pack(); err == nil {
				entry.RawQuery = packed
			}
			if packed, err := recorder.Msg.Pack(); err == nil {
				entry.RawResponse = packed
			}
		}
	} else {
		entry.RCode = dns.RcodeToString[rcode]
	}

	return entry
}

// parseAnswers extracts structured records from the answer section.
func parseAnswers(answers []dns.RR) []AnswerRecord {
	records := make([]AnswerRecord, 0, len(answers))
	for _, rr := range answers {
		hdr := rr.Header()
		// Extract content by removing the header prefix from the string representation
		full := rr.String()
		parts := strings.SplitN(full, "\t", 5)
		content := ""
		if len(parts) == 5 {
			content = parts[4]
		}

		records = append(records, AnswerRecord{
			Name:    hdr.Name,
			Type:    dns.TypeToString[hdr.Rrtype],
			Content: content,
			TTL:     int(hdr.Ttl),
		})
	}
	return records
}

// buildFlags returns a comma-separated string of DNS response flags.
func buildFlags(msg *dns.Msg) string {
	var flags []string
	if msg.Response {
		flags = append(flags, "qr")
	}
	if msg.Authoritative {
		flags = append(flags, "aa")
	}
	if msg.Truncated {
		flags = append(flags, "tc")
	}
	if msg.RecursionDesired {
		flags = append(flags, "rd")
	}
	if msg.RecursionAvailable {
		flags = append(flags, "ra")
	}
	if msg.AuthenticatedData {
		flags = append(flags, "ad")
	}
	if msg.CheckingDisabled {
		flags = append(flags, "cd")
	}
	return strings.Join(flags, ",")
}

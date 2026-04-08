package surreallog

import (
	"context"
	"net"
	"testing"

	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/plugin/test"
	"github.com/miekg/dns"
)

// TestServeDNS verifies that the plugin captures queries and passes them through.
func TestServeDNS(t *testing.T) {
	config := defaultConfig()
	config.BufferSize = 100

	writer := NewWriter(config)

	// Create a handler that returns a fixed A record response
	nextHandler := test.HandlerFunc(func(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
		m := new(dns.Msg)
		m.SetReply(r)
		m.Authoritative = true
		m.Answer = append(m.Answer, test.A("example.com. 300 IN A 1.2.3.4"))
		w.WriteMsg(m)
		return dns.RcodeSuccess, nil
	})

	sl := &SurrealLog{
		Next:   nextHandler,
		writer: writer,
		config: config,
	}

	ctx := context.TODO()
	r := new(dns.Msg)
	r.SetQuestion("example.com.", dns.TypeA)

	rec := dnstest.NewRecorder(&test.ResponseWriter{})

	rcode, err := sl.ServeDNS(ctx, rec, r)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if rcode != dns.RcodeSuccess {
		t.Fatalf("Expected RcodeSuccess, got %d", rcode)
	}

	// Verify an entry was sent to the channel
	select {
	case entry := <-writer.logChan:
		if entry.QName != "example.com." {
			t.Errorf("Expected qname 'example.com.', got '%s'", entry.QName)
		}
		if entry.QType != "A" {
			t.Errorf("Expected qtype 'A', got '%s'", entry.QType)
		}
		if entry.RCode != "NOERROR" {
			t.Errorf("Expected rcode 'NOERROR', got '%s'", entry.RCode)
		}
		if entry.AnswerCount != 1 {
			t.Errorf("Expected 1 answer, got %d", entry.AnswerCount)
		}
		if len(entry.Answers) != 1 {
			t.Fatalf("Expected 1 parsed answer, got %d", len(entry.Answers))
		}
		if entry.Answers[0].Content != "1.2.3.4" {
			t.Errorf("Expected answer content '1.2.3.4', got '%s'", entry.Answers[0].Content)
		}
	default:
		t.Fatal("Expected log entry in channel, got none")
	}
}

// TestBuildInsertQuery verifies the SurrealQL INSERT is well-formed.
func TestBuildInsertQuery(t *testing.T) {
	config := defaultConfig()
	writer := NewWriter(config)

	entries := []LogEntry{
		{
			QName:       "test.com.",
			QType:       "A",
			QClass:      "IN",
			ClientIP:    "10.0.0.1",
			ClientPort:  12345,
			Protocol:    "udp",
			RCode:       "NOERROR",
			AnswerCount: 1,
			Answers: []AnswerRecord{
				{Name: "test.com.", Type: "A", Content: "1.2.3.4", TTL: 300},
			},
		},
	}

	query := writer.buildInsertQuery(entries)

	if query == "" {
		t.Fatal("Expected non-empty query")
	}
	if !contains(query, "INSERT INTO dns_query") {
		t.Error("Expected INSERT INTO dns_query")
	}
	if !contains(query, "test.com.") {
		t.Error("Expected qname in query")
	}
	if !contains(query, "1.2.3.4") {
		t.Error("Expected answer content in query")
	}
}

// TestDropWhenBufferFull verifies entries are dropped, not blocking.
func TestDropWhenBufferFull(t *testing.T) {
	config := defaultConfig()
	config.BufferSize = 1

	writer := NewWriter(config)

	// Fill the buffer
	writer.logChan <- LogEntry{QName: "first.com."}

	// This should be dropped, not block
	select {
	case writer.logChan <- LogEntry{QName: "second.com."}:
		t.Fatal("Expected channel to be full")
	default:
		// Correct — channel full, would have been dropped in ServeDNS
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && searchString(s, sub)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Ensure net import is used (ResponseWriter needs it)
var _ net.Addr

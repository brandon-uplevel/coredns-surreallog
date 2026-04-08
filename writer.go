package surreallog

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	clog "github.com/coredns/coredns/plugin/pkg/log"
)

var log = clog.NewWithPlugin("surreallog")

// Writer handles async batched writes to SurrealDB.
type Writer struct {
	config  *Config
	logChan chan LogEntry
	done    chan struct{}
	client  *http.Client
}

// NewWriter creates a new Writer with the given config.
func NewWriter(config *Config) *Writer {
	return &Writer{
		config:  config,
		logChan: make(chan LogEntry, config.BufferSize),
		done:    make(chan struct{}),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// Start begins the background writer goroutine.
func (w *Writer) Start() error {
	log.Infof("Starting surreallog writer: %s (ns=%s, db=%s, batch=%d, flush=%s)",
		w.config.URL, w.config.Namespace, w.config.Database,
		w.config.BatchSize, w.config.FlushInterval)
	go w.run()
	return nil
}

// Stop flushes remaining entries and shuts down the writer.
func (w *Writer) Stop() error {
	log.Info("Stopping surreallog writer, flushing remaining entries...")
	close(w.done)
	return nil
}

// run is the main loop that collects entries and flushes them in batches.
func (w *Writer) run() {
	batch := make([]LogEntry, 0, w.config.BatchSize)
	ticker := time.NewTicker(w.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case entry := <-w.logChan:
			batch = append(batch, entry)
			if len(batch) >= w.config.BatchSize {
				w.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				w.flush(batch)
				batch = batch[:0]
			}
		case <-w.done:
			// Drain remaining entries from channel
			for {
				select {
				case entry := <-w.logChan:
					batch = append(batch, entry)
				default:
					if len(batch) > 0 {
						w.flush(batch)
					}
					log.Info("Surreallog writer stopped")
					return
				}
			}
		}
	}
}

// flush sends a batch of log entries to SurrealDB via HTTP.
func (w *Writer) flush(batch []LogEntry) {
	if len(batch) == 0 {
		return
	}

	query := w.buildInsertQuery(batch)

	req, err := http.NewRequest("POST", w.config.URL+"/sql", strings.NewReader(query))
	if err != nil {
		log.Errorf("Failed to create request: %v", err)
		errorsTotal.Inc()
		return
	}

	req.Header.Set("Content-Type", "application/surql")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("surreal-ns", w.config.Namespace)
	req.Header.Set("surreal-db", w.config.Database)

	// Basic auth
	if w.config.Username != "" {
		req.SetBasicAuth(w.config.Username, w.config.Password)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		log.Errorf("Failed to write %d entries to SurrealDB: %v", len(batch), err)
		errorsTotal.Inc()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Errorf("SurrealDB returned %d: %s", resp.StatusCode, string(body))
		errorsTotal.Inc()
		return
	}

	flushedTotal.Add(float64(len(batch)))
}

// buildInsertQuery constructs a SurrealQL INSERT statement for a batch of entries.
func (w *Writer) buildInsertQuery(batch []LogEntry) string {
	var buf bytes.Buffer

	buf.WriteString("INSERT INTO dns_query [")

	for i, entry := range batch {
		if i > 0 {
			buf.WriteString(",")
		}

		buf.WriteString(fmt.Sprintf(
			`{timestamp:d'%s',server:'%s',client_ip:'%s',client_port:%d,qname:'%s',qtype:'%s',qclass:'%s',protocol:'%s',query_size:%d,rcode:'%s',answer_count:%d,response_size:%d,latency_us:%d,flags:'%s'`,
			entry.Timestamp.Format(time.RFC3339Nano),
			escape(entry.Server),
			escape(entry.ClientIP),
			entry.ClientPort,
			escape(entry.QName),
			escape(entry.QType),
			escape(entry.QClass),
			escape(entry.Protocol),
			entry.QuerySize,
			escape(entry.RCode),
			entry.AnswerCount,
			entry.ResponseSize,
			entry.LatencyUs,
			escape(entry.Flags),
		))

		// Answers array
		if len(entry.Answers) > 0 {
			buf.WriteString(",answers:[")
			for j, a := range entry.Answers {
				if j > 0 {
					buf.WriteString(",")
				}
				buf.WriteString(fmt.Sprintf(
					`{name:'%s',type:'%s',content:'%s',ttl:%d}`,
					escape(a.Name), escape(a.Type), escape(a.Content), a.TTL,
				))
			}
			buf.WriteString("]")
		}

		// Raw packets (base64 encoded)
		if len(entry.RawQuery) > 0 {
			buf.WriteString(fmt.Sprintf(",raw_query:encoding::base64::decode('%s')",
				base64.StdEncoding.EncodeToString(entry.RawQuery)))
		}
		if len(entry.RawResponse) > 0 {
			buf.WriteString(fmt.Sprintf(",raw_response:encoding::base64::decode('%s')",
				base64.StdEncoding.EncodeToString(entry.RawResponse)))
		}

		buf.WriteString("}")
	}

	buf.WriteString("];")
	return buf.String()
}

// escape single quotes in string values for SurrealQL.
func escape(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

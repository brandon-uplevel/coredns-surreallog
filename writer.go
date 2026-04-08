package surreallog

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
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

	// JWT token auth
	token   string
	tokenMu sync.RWMutex
	tokenExp time.Time
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

	// Initial signin
	if err := w.signin(); err != nil {
		log.Errorf("Initial SurrealDB signin failed: %v (will retry on flush)", err)
	}

	go w.run()
	return nil
}

// Stop flushes remaining entries and shuts down the writer.
func (w *Writer) Stop() error {
	log.Info("Stopping surreallog writer, flushing remaining entries...")
	close(w.done)
	return nil
}

// signin authenticates with SurrealDB and stores the JWT token.
func (w *Writer) signin() error {
	body, _ := json.Marshal(map[string]string{
		"ns":   w.config.Namespace,
		"db":   w.config.Database,
		"user": w.config.Username,
		"pass": w.config.Password,
	})

	req, err := http.NewRequest("POST", w.config.URL+"/signin", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create signin request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("signin request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("signin returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("parse signin response: %w", err)
	}

	if result.Token == "" {
		return fmt.Errorf("signin returned empty token")
	}

	w.tokenMu.Lock()
	w.token = result.Token
	w.tokenExp = time.Now().Add(50 * time.Minute) // Refresh before 1h expiry
	w.tokenMu.Unlock()

	log.Info("SurrealDB signin successful, token acquired")
	return nil
}

// getToken returns the current token, refreshing if needed.
func (w *Writer) getToken() string {
	w.tokenMu.RLock()
	token := w.token
	exp := w.tokenExp
	w.tokenMu.RUnlock()

	if time.Now().After(exp) {
		if err := w.signin(); err != nil {
			log.Errorf("Token refresh failed: %v", err)
			return token // Use old token, might still work
		}
		w.tokenMu.RLock()
		token = w.token
		w.tokenMu.RUnlock()
	}

	return token
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
	token := w.getToken()

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

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else {
		// Fallback to basic auth (for root-level users)
		req.SetBasicAuth(w.config.Username, w.config.Password)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		log.Errorf("Failed to write %d entries to SurrealDB: %v", len(batch), err)
		errorsTotal.Inc()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		// Token expired or invalid — re-signin and retry once
		body, _ := io.ReadAll(resp.Body)
		log.Warningf("SurrealDB 401, re-authenticating: %s", string(body))
		if err := w.signin(); err != nil {
			log.Errorf("Re-signin failed: %v", err)
			errorsTotal.Inc()
			return
		}
		// Retry the flush
		w.flushWithToken(batch, w.token)
		return
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Errorf("SurrealDB returned %d: %s", resp.StatusCode, string(body))
		errorsTotal.Inc()
		return
	}

	flushedTotal.Add(float64(len(batch)))
}

// flushWithToken retries a flush with a specific token (after re-auth).
func (w *Writer) flushWithToken(batch []LogEntry, token string) {
	query := w.buildInsertQuery(batch)

	req, err := http.NewRequest("POST", w.config.URL+"/sql", strings.NewReader(query))
	if err != nil {
		log.Errorf("Failed to create retry request: %v", err)
		errorsTotal.Inc()
		return
	}

	req.Header.Set("Content-Type", "application/surql")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("surreal-ns", w.config.Namespace)
	req.Header.Set("surreal-db", w.config.Database)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := w.client.Do(req)
	if err != nil {
		log.Errorf("Retry flush failed: %v", err)
		errorsTotal.Inc()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Errorf("Retry flush returned %d: %s", resp.StatusCode, string(body))
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

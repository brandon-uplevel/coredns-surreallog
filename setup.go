package surreallog

import (
	"strconv"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
)

func init() { plugin.Register("surreallog", setup) }

// Config holds the parsed Corefile configuration for surreallog.
type Config struct {
	URL           string        // SurrealDB HTTP endpoint
	Namespace     string        // SurrealDB namespace
	Database      string        // SurrealDB database
	Username      string        // SurrealDB auth username
	Password      string        // SurrealDB auth password
	ServerID      string        // "ns1" or "ns2" — identifies which nameserver
	BatchSize     int           // Flush after this many entries
	FlushInterval time.Duration // Flush after this interval
	BufferSize    int           // Channel buffer size (entries dropped if full)
	LogAnswers    bool          // Include parsed answer records
	LogRaw        bool          // Include raw wire-format packets
}

func defaultConfig() *Config {
	return &Config{
		URL:           "http://localhost:8000",
		Namespace:     "hellojade_dns",
		Database:      "logs",
		Username:      "root",
		Password:      "root",
		ServerID:      "ns1",
		BatchSize:     100,
		FlushInterval: time.Second,
		BufferSize:    10000,
		LogAnswers:    true,
		LogRaw:        false,
	}
}

// setup parses the Corefile and configures the plugin.
//
// Corefile syntax:
//
//	surreallog {
//	    url http://surrealdb:8000
//	    namespace hellojade_dns
//	    database logs
//	    username root
//	    password secret
//	    server_id ns1
//	    batch_size 100
//	    flush_interval 1s
//	    buffer_size 10000
//	    log_answers true
//	    log_raw false
//	}
func setup(c *caddy.Controller) error {
	config, err := parseConfig(c)
	if err != nil {
		return plugin.Error("surreallog", err)
	}

	writer := NewWriter(config)

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		return &SurrealLog{
			Next:   next,
			writer: writer,
			config: config,
		}
	})

	c.OnStartup(func() error {
		return writer.Start()
	})

	c.OnShutdown(func() error {
		return writer.Stop()
	})

	return nil
}

func parseConfig(c *caddy.Controller) (*Config, error) {
	config := defaultConfig()

	for c.Next() {
		// Skip the plugin name "surreallog"
		for c.NextBlock() {
			switch c.Val() {
			case "url":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				config.URL = c.Val()
			case "namespace":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				config.Namespace = c.Val()
			case "database":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				config.Database = c.Val()
			case "username":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				config.Username = c.Val()
			case "password":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				config.Password = c.Val()
			case "server_id":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				config.ServerID = c.Val()
			case "batch_size":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				n, err := strconv.Atoi(c.Val())
				if err != nil {
					return nil, c.Errf("invalid batch_size: %s", c.Val())
				}
				config.BatchSize = n
			case "flush_interval":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				d, err := time.ParseDuration(c.Val())
				if err != nil {
					return nil, c.Errf("invalid flush_interval: %s", c.Val())
				}
				config.FlushInterval = d
			case "buffer_size":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				n, err := strconv.Atoi(c.Val())
				if err != nil {
					return nil, c.Errf("invalid buffer_size: %s", c.Val())
				}
				config.BufferSize = n
			case "log_answers":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				b, err := strconv.ParseBool(c.Val())
				if err != nil {
					return nil, c.Errf("invalid log_answers: %s", c.Val())
				}
				config.LogAnswers = b
			case "log_raw":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				b, err := strconv.ParseBool(c.Val())
				if err != nil {
					return nil, c.Errf("invalid log_raw: %s", c.Val())
				}
				config.LogRaw = b
			default:
				return nil, c.Errf("unknown surreallog option: %s", c.Val())
			}
		}
	}

	return config, nil
}

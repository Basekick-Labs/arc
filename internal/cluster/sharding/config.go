package sharding

import "time"

// Config holds sharding configuration.
type Config struct {
	// Enabled enables sharding for the cluster
	Enabled bool

	// NumShards is the total number of shards (default: 3)
	NumShards int

	// ShardKey determines how data is partitioned
	// Supported values: "database" (default), "measurement"
	ShardKey string

	// ReplicationFactor is the number of copies of each shard (default: 3)
	ReplicationFactor int

	// RouteTimeout is the timeout for forwarding requests (default: 5s)
	RouteTimeout time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Enabled:           false,
		NumShards:         3,
		ShardKey:          "database",
		ReplicationFactor: 3,
		RouteTimeout:      5 * time.Second,
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.NumShards < 1 {
		c.NumShards = 1
	}
	if c.ReplicationFactor < 1 {
		c.ReplicationFactor = 1
	}
	if c.ShardKey == "" {
		c.ShardKey = "database"
	}
	if c.RouteTimeout <= 0 {
		c.RouteTimeout = 5 * time.Second
	}
	return nil
}

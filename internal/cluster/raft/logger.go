package raft

import (
	"io"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/rs/zerolog"
)

// zerologAdapter adapts zerolog to hclog.Logger interface used by hashicorp/raft.
type zerologAdapter struct {
	log  zerolog.Logger
	name string
}

// newZerologAdapter creates a new hclog.Logger that writes to zerolog.
func newZerologAdapter(log zerolog.Logger, name string) hclog.Logger {
	return &zerologAdapter{
		log:  log.With().Str("component", name).Logger(),
		name: name,
	}
}

func (z *zerologAdapter) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Trace, hclog.Debug:
		z.Debug(msg, args...)
	case hclog.Info:
		z.Info(msg, args...)
	case hclog.Warn:
		z.Warn(msg, args...)
	case hclog.Error:
		z.Error(msg, args...)
	default:
		z.Info(msg, args...)
	}
}

func (z *zerologAdapter) Trace(msg string, args ...interface{}) {
	z.log.Trace().Fields(argsToFields(args)).Msg(msg)
}

func (z *zerologAdapter) Debug(msg string, args ...interface{}) {
	z.log.Debug().Fields(argsToFields(args)).Msg(msg)
}

func (z *zerologAdapter) Info(msg string, args ...interface{}) {
	z.log.Info().Fields(argsToFields(args)).Msg(msg)
}

func (z *zerologAdapter) Warn(msg string, args ...interface{}) {
	z.log.Warn().Fields(argsToFields(args)).Msg(msg)
}

func (z *zerologAdapter) Error(msg string, args ...interface{}) {
	z.log.Error().Fields(argsToFields(args)).Msg(msg)
}

func (z *zerologAdapter) IsTrace() bool { return true }
func (z *zerologAdapter) IsDebug() bool { return true }
func (z *zerologAdapter) IsInfo() bool  { return true }
func (z *zerologAdapter) IsWarn() bool  { return true }
func (z *zerologAdapter) IsError() bool { return true }

func (z *zerologAdapter) ImpliedArgs() []interface{} { return nil }

func (z *zerologAdapter) With(args ...interface{}) hclog.Logger {
	fields := argsToFields(args)
	return &zerologAdapter{
		log:  z.log.With().Fields(fields).Logger(),
		name: z.name,
	}
}

func (z *zerologAdapter) Name() string { return z.name }

func (z *zerologAdapter) Named(name string) hclog.Logger {
	return &zerologAdapter{
		log:  z.log.With().Str("sub", name).Logger(),
		name: z.name + "." + name,
	}
}

func (z *zerologAdapter) ResetNamed(name string) hclog.Logger {
	return &zerologAdapter{
		log:  z.log,
		name: name,
	}
}

func (z *zerologAdapter) SetLevel(level hclog.Level) {}

func (z *zerologAdapter) GetLevel() hclog.Level {
	return hclog.Info
}

func (z *zerologAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return log.New(z.StandardWriter(opts), "", 0)
}

func (z *zerologAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return z.log
}

// argsToFields converts hclog-style args (key, value, key, value, ...) to a map.
func argsToFields(args []interface{}) map[string]interface{} {
	if len(args) == 0 {
		return nil
	}
	fields := make(map[string]interface{})
	for i := 0; i < len(args)-1; i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}
		fields[key] = args[i+1]
	}
	return fields
}

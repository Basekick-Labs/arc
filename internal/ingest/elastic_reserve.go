package ingest

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/basekick-labs/arc/internal/metrics"
)

// RuntimeElasticReserveConfig controls the in-memory overflow reserve. The
// capacity is an accounting budget in records. The reserve preallocates one
// task pointer slot per record; queued payloads are retained by reference and
// are not copied into a second representation.
type RuntimeElasticReserveConfig struct {
	Enabled         bool  `json:"enabled"`
	CapacityRecords int64 `json:"capacity_records"`
}

func validateRuntimeElasticReserveConfig(cfg RuntimeElasticReserveConfig) error {
	if cfg.CapacityRecords < 0 {
		return fmt.Errorf("capacity_records must not be negative")
	}
	if cfg.Enabled && cfg.CapacityRecords == 0 {
		return fmt.Errorf("capacity_records must be greater than zero when enabled")
	}
	if cfg.CapacityRecords > int64(math.MaxInt) {
		return fmt.Errorf("capacity_records exceeds the platform limit")
	}
	return nil
}

// ElasticReserveConfig reports process-local reserve settings.
func (b *ArrowBuffer) ElasticReserveConfig() RuntimeElasticReserveConfig {
	b.elasticReserveMu.Lock()
	defer b.elasticReserveMu.Unlock()
	return RuntimeElasticReserveConfig{
		Enabled:         b.elasticReserveEnabled.Load(),
		CapacityRecords: b.elasticReserveCapacity.Load(),
	}
}

func (b *ArrowBuffer) ElasticReserveUsedRecords() int64 {
	return b.elasticReserveRecords.Load()
}

// ConfigureElasticReserve changes the queue-full reserve on the control path.
// Existing tasks must drain before a smaller capacity or disable can apply.
func (b *ArrowBuffer) ConfigureElasticReserve(cfg RuntimeElasticReserveConfig) error {
	return b.ConfigureElasticReserveWithPersistence(cfg, nil)
}

// ConfigureElasticReserveWithPersistence serializes the SQLite mutation with
// queue-full reserve admission. The callback runs only after validation and
// capacity checks succeed, and the live atomics change only after the callback
// succeeds. Normal record writes do not acquire this lock.
func (b *ArrowBuffer) ConfigureElasticReserveWithPersistence(cfg RuntimeElasticReserveConfig, persist func() error) error {
	if b == nil {
		return fmt.Errorf("arrow buffer is unavailable")
	}
	if err := validateRuntimeElasticReserveConfig(cfg); err != nil {
		return err
	}
	b.elasticReserveMu.Lock()
	defer b.elasticReserveMu.Unlock()
	if b.closing.Load() {
		return fmt.Errorf("cannot configure elastic reserve while ArrowBuffer is closing")
	}
	used := b.elasticReserveRecords.Load()
	if used > cfg.CapacityRecords {
		return fmt.Errorf("capacity_records (%d) is below current reserve occupancy (%d)", cfg.CapacityRecords, used)
	}
	if !cfg.Enabled && used > 0 {
		return fmt.Errorf("cannot disable elastic reserve while %d records are pending", used)
	}
	var preparedTasks []*flushTask
	if cfg.Enabled && cap(b.elasticReserveTasks) != int(cfg.CapacityRecords) {
		needed, err := elasticReserveSlotsBytes(cfg.CapacityRecords)
		if err != nil {
			return err
		}
		available, known := availableMemoryBytes()
		if !known {
			return fmt.Errorf("cannot verify available memory for elastic reserve activation")
		}
		oldBytes := uint64(cap(b.elasticReserveTasks)) * uint64(unsafe.Sizeof((*flushTask)(nil)))
		if needed > available || oldBytes > available-needed {
			return fmt.Errorf("insufficient memory headroom for elastic reserve: need %d bytes while %d bytes are available", needed+oldBytes, available)
		}
		preparedTasks = make([]*flushTask, len(b.elasticReserveTasks), int(cfg.CapacityRecords))
		copy(preparedTasks, b.elasticReserveTasks)
	}
	if persist != nil {
		if err := persist(); err != nil {
			return err
		}
	}
	if cfg.Enabled {
		if preparedTasks != nil {
			b.elasticReserveTasks = preparedTasks
		}
	} else {
		b.elasticReserveTasks = nil
	}
	b.elasticReserveCapacity.Store(cfg.CapacityRecords)
	b.elasticReserveEnabled.Store(cfg.Enabled)
	metrics.Get().SetBufferElasticReserveEnabled(cfg.Enabled)
	metrics.Get().SetBufferElasticReserveCapacity(cfg.CapacityRecords)
	metrics.Get().SetBufferElasticReserveRecords(used)
	return nil
}

func (b *ArrowBuffer) admitElasticTask(task flushTask) bool {
	if task.recordCount <= 0 {
		return false
	}
	b.elasticReserveMu.Lock()
	defer b.elasticReserveMu.Unlock()
	if b.closing.Load() || !b.elasticReserveEnabled.Load() {
		return false
	}
	used := b.elasticReserveRecords.Load()
	capacity := b.elasticReserveCapacity.Load()
	if int64(task.recordCount) > capacity-used {
		return false
	}
	if len(b.elasticReserveTasks) == cap(b.elasticReserveTasks) {
		return false
	}
	queuedTask := task
	b.elasticReserveTasks = append(b.elasticReserveTasks, &queuedTask)
	used += int64(task.recordCount)
	b.elasticReserveRecords.Store(used)
	metrics.Get().SetBufferElasticReserveRecords(used)
	return true
}

func elasticReserveSlotsBytes(capacityRecords int64) (uint64, error) {
	if capacityRecords <= 0 || capacityRecords > int64(math.MaxInt) {
		return 0, fmt.Errorf("capacity_records is outside the supported allocation range")
	}
	slotBytes := uint64(unsafe.Sizeof((*flushTask)(nil)))
	capacity := uint64(capacityRecords)
	if capacity > math.MaxUint64/slotBytes {
		return 0, fmt.Errorf("elastic reserve allocation size overflows")
	}
	return capacity * slotBytes, nil
}

// drainElasticReserve moves at most one reserve task back into flushQueue. It
// is called immediately after a worker receives a queue task, when that receive
// has made a slot available. A non-blocking send keeps this worker event from
// waiting behind concurrent producers.
func (b *ArrowBuffer) drainElasticReserve() {
	b.elasticReserveMu.Lock()
	defer b.elasticReserveMu.Unlock()
	if len(b.elasticReserveTasks) == 0 {
		return
	}
	task := b.elasticReserveTasks[0]
	select {
	case b.flushQueue <- *task:
		copy(b.elasticReserveTasks, b.elasticReserveTasks[1:])
		last := len(b.elasticReserveTasks) - 1
		b.elasticReserveTasks[last] = nil
		b.elasticReserveTasks = b.elasticReserveTasks[:last]
		used := b.elasticReserveRecords.Add(-int64(task.recordCount))
		metrics.Get().SetBufferElasticReserveRecords(used)
		metrics.Get().SetBufferQueueDepth(b.queueDepth.Add(1))
		metrics.Get().IncBufferFlushQueueEnqueued(int64(task.recordCount))
	default:
	}
}

// takeElasticReserveTasks transfers every still-owned task to shutdown
// accounting after workers have stopped.
func (b *ArrowBuffer) takeElasticReserveTasks() []*flushTask {
	b.elasticReserveMu.Lock()
	defer b.elasticReserveMu.Unlock()
	tasks := b.elasticReserveTasks
	b.elasticReserveTasks = nil
	b.elasticReserveRecords.Store(0)
	metrics.Get().SetBufferElasticReserveRecords(0)
	return tasks
}

package nettrace

import (
	"slices"
	"sync"
)

// evictingMap is a bounded, ordered in-memory store designed to cache
// recent metadata entries (e.g., connections, dials, http requests).
//
// 🔹 Purpose:
//   - Acts as a memory-efficient ring buffer to limit RAM usage.
//   - Retains the most recent N entries (configurable).
//   - Preserves insertion order to evict the oldest item when the limit is exceeded.
//
// 🔹 Behavior:
//   - When the capacity is reached, the oldest entry is evicted.
//   - Evicted items are batched and asynchronously flushed to BoltDB for persistence.
//   - Supports efficient iteration and deletion while keeping a separate 'order' slice.
//
// This structure is part of a hybrid trace metadata solution, where evictingMap holds
// recent data in memory and older data is stored in key/value store on disk, balancing performance with resource use.
type evictingMap struct {
	store       map[TraceID]interface{}
	order       []TraceID
	limit       int
	bucket      string
	batchBuffer []finalizedTrace
	mutex       sync.Mutex

	flushThreshold int
	flushFn        func(batch []finalizedTrace)
}

func newEvictingMap(limit int, bucket string, flushThreshold int, flushFn func([]finalizedTrace)) *evictingMap {
	return &evictingMap{
		store:          make(map[TraceID]interface{}),
		order:          make([]TraceID, 0, limit),
		limit:          limit,
		bucket:         bucket,
		flushThreshold: flushThreshold,
		flushFn:        flushFn,
	}
}

// evicted maps
func (m *evictingMap) Set(key TraceID, value interface{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.store[key]; exists {
		m.store[key] = value
		return
	}

	// Evict if over limit
	if len(m.order) >= m.limit {
		oldest := m.order[0]
		m.order = m.order[1:]

		oldVal := m.store[oldest]
		delete(m.store, oldest)

		m.batchBuffer = append(m.batchBuffer, finalizedTrace{
			Bucket: m.bucket,
			Key:    oldest,
			Value:  oldVal,
		})

		// Flush if enough evictions accumulated
		if len(m.batchBuffer) >= m.flushThreshold {
			batch := m.batchBuffer
			m.batchBuffer = nil // clear before flushing
			go m.flushFn(batch) // async flush
		}
	}

	m.order = append(m.order, key)
	m.store[key] = value
}

func (m *evictingMap) Len() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return len(m.store)
}

func (m *evictingMap) Get(key TraceID) (interface{}, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	val, ok := m.store[key]
	return val, ok
}

func (m *evictingMap) Delete(key TraceID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.store[key]; !exists {
		return
	}
	delete(m.store, key)

	for i, id := range m.order {
		if id == key {
			m.order = slices.Delete(m.order, i, i+1)
			break
		}
	}
}

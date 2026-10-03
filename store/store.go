// Package store is an experimental shared, compressed Whisper-compatible store.
// It intentionally has no go-carbon dependency.
package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	whisper "github.com/go-graphite/go-whisper"
)

var (
	ErrNotFound = errors.New("metric not found")
	ErrExists   = errors.New("metric already exists")
	ErrConflict = errors.New("metric changed or configuration is incompatible")
)

// Options constrains memory used by Pebble. Commits are always WAL-synchronised.
type Options struct {
	CacheSize    int64
	MemTableSize uint64
	Now          func() time.Time
}

type MetricConfig struct {
	Name              string
	Retentions        []whisper.Retention
	AggregationMethod whisper.AggregationMethod
	XFilesFactor      float32
}

type Metadata struct {
	MetricConfig
	ID         uint64
	Generation uint64
	Revision   uint64
}

type Series struct {
	Metadata  Metadata
	FromTime  int
	UntilTime int
	Step      int
	Values    []float64
}

// Archive is an archive-preserving snapshot. Points use their aligned timestamp.
type Archive struct {
	Retention whisper.Retention
	Points    []whisper.TimeSeriesPoint
}

type Snapshot struct {
	Metadata Metadata
	Archives []Archive
}

type Store struct {
	db       *pebble.DB
	cache    *pebble.Cache
	now      func() time.Time
	mu       sync.Mutex // serialises catalog IDs and create/delete/replace
	metricMu [256]sync.Mutex
}

func Open(dir string, options Options) (*Store, error) {
	if dir == "" {
		return nil, errors.New("store directory is empty")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	pOpts := &pebble.Options{MemTableSize: options.MemTableSize}
	if pOpts.MemTableSize == 0 {
		pOpts.MemTableSize = 64 << 20
	}
	s := &Store{now: options.Now}
	if options.CacheSize > 0 {
		s.cache = pebble.NewCache(options.CacheSize)
		pOpts.Cache = s.cache
	}
	db, err := pebble.Open(dir, pOpts)
	if err != nil {
		if s.cache != nil {
			s.cache.Unref()
		}
		return nil, fmt.Errorf("open pebble store: %w", err)
	}
	s.db = db
	return s, nil
}

func (s *Store) Close() error {
	err := s.db.Close()
	if s.cache != nil {
		s.cache.Unref()
	}
	return err
}

// Flush makes all current mutable tables durable as sorted files. It is useful
// to benchmarks and maintenance tooling; normal writes are already WAL-synced.
func (s *Store) Flush() error {
	if err := s.db.Flush(); err != nil {
		return fmt.Errorf("flush store: %w", err)
	}
	return nil
}

// Compact rewrites the entire key space. Callers should rate-limit it.
func (s *Store) Compact() error {
	if err := s.db.Compact([]byte{}, []byte{0xff}, true); err != nil {
		return fmt.Errorf("compact store: %w", err)
	}
	return nil
}

// Stats reports stable physical store bytes without exposing the Pebble handle.
type Stats struct {
	DiskBytes     uint64
	WALBytes      uint64
	MemTableBytes uint64
}

func (s *Store) Stats() Stats {
	m := s.db.Metrics()
	return Stats{DiskBytes: uint64(m.DiskSpaceUsage()), WALBytes: uint64(m.WAL.Size), MemTableBytes: uint64(m.MemTable.Size)}
}

func (s *Store) lockMetric(name string) func() {
	var h byte
	for i := range name {
		h = h*33 + name[i]
	}
	m := &s.metricMu[h]
	m.Lock()
	return m.Unlock
}

func catalogKey(name string) []byte { return append([]byte("m/"), []byte(name)...) }
func sequenceKey() []byte           { return []byte("z/sequence") }

func (s *Store) Create(ctx context.Context, config MetricConfig) (Metadata, error) {
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := validate(config); err != nil {
		return Metadata{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, closer, err := s.db.Get(catalogKey(config.Name)); err == nil {
		closer.Close()
		return Metadata{}, fmt.Errorf("%w: %s", ErrExists, config.Name)
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return Metadata{}, fmt.Errorf("read catalog: %w", err)
	}
	id, err := s.nextID()
	if err != nil {
		return Metadata{}, err
	}
	m := Metadata{MetricConfig: cloneConfig(config), ID: id, Generation: 1, Revision: 1}
	b, err := marshalMetadata(m)
	if err != nil {
		return Metadata{}, fmt.Errorf("encode metadata: %w", err)
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(catalogKey(config.Name), b, nil); err != nil {
		return Metadata{}, fmt.Errorf("catalog metric: %w", err)
	}
	if err := batch.Set(sequenceKey(), uint64Bytes(id), nil); err != nil {
		return Metadata{}, fmt.Errorf("catalog sequence: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return Metadata{}, fmt.Errorf("sync create: %w", err)
	}
	return m, nil
}

func (s *Store) nextID() (uint64, error) {
	v, closer, err := s.db.Get(sequenceKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return 1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read sequence: %w", err)
	}
	defer closer.Close()
	return binary.BigEndian.Uint64(v) + 1, nil
}

func (s *Store) Metadata(ctx context.Context, name string) (Metadata, error) {
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	return s.metadata(name)
}

func (s *Store) metadata(name string) (Metadata, error) {
	return metadataFrom(s.db, name)
}

func metadataFrom(reader pebble.Reader, name string) (Metadata, error) {
	v, closer, err := reader.Get(catalogKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return Metadata{}, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if err != nil {
		return Metadata{}, fmt.Errorf("read catalog: %w", err)
	}
	defer closer.Close()
	return unmarshalMetadata(v)
}

func (s *Store) List(ctx context.Context, prefix string) ([]Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("m/"), UpperBound: prefixEnd([]byte("m/"))})
	if err != nil {
		return nil, fmt.Errorf("list catalog: %w", err)
	}
	defer it.Close()
	var result []Metadata
	for it.First(); it.Valid(); it.Next() {
		m, err := unmarshalMetadata(it.Value())
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(m.Name, prefix) {
			result = append(result, m)
		}
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("iterate catalog: %w", err)
	}
	return result, nil
}

// Update is the classic single-point API: future and expired points are rejected.
func (s *Store) Update(ctx context.Context, name string, value float64, timestamp int) error {
	unlock := s.lockMetric(name)
	defer unlock()
	m, err := s.Metadata(ctx, name)
	if err != nil {
		return err
	}
	diff := int(s.now().Unix()) - timestamp
	if diff < 0 || diff >= maxRetention(m.Retentions) {
		return errors.New("timestamp not covered by any archives in this database")
	}
	return s.update(ctx, m, []whisper.TimeSeriesPoint{{Time: timestamp, Value: value}}, -1)
}

// UpdateMany follows Whisper's batch admission: future points are accepted and
// expired points are ignored. Same-slot duplicates retain the last input value.
func (s *Store) UpdateMany(ctx context.Context, name string, points []whisper.TimeSeriesPoint) error {
	return s.updateByName(ctx, name, points, -1)
}

// UpdateManyForArchive writes directly into a selected archive. It deliberately
// does not propagate, which permits archive-preserving imports.
func (s *Store) UpdateManyForArchive(ctx context.Context, name string, points []whisper.TimeSeriesPoint, targetRetention int) error {
	return s.updateByName(ctx, name, points, targetRetention)
}

func (s *Store) updateByName(ctx context.Context, name string, points []whisper.TimeSeriesPoint, targetRetention int) error {
	unlock := s.lockMetric(name)
	defer unlock()
	m, err := s.Metadata(ctx, name)
	if err != nil {
		return err
	}
	return s.update(ctx, m, points, targetRetention)
}

func (s *Store) update(ctx context.Context, m Metadata, points []whisper.TimeSeriesPoint, targetRetention int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b := s.db.NewIndexedBatch()
	defer b.Close()
	now := int(s.now().Unix())
	if targetRetention >= 0 {
		archive := targetArchive(m.Retentions, 0, targetRetention)
		if archive < 0 {
			return fmt.Errorf("target retention %d not found", targetRetention)
		}
		for _, p := range points {
			if err := setPoint(b, m, archive, align(p.Time, m.Retentions[archive].SecondsPerPoint()), p.Value); err != nil {
				return err
			}
		}
	} else {
		// This preserves classic UpdateMany's newest-first routing and exact
		// retention-boundary behaviour.
		remaining := append([]whisper.TimeSeriesPoint(nil), points...)
		for i := 0; i < len(remaining)/2; i++ {
			remaining[i], remaining[len(remaining)-i-1] = remaining[len(remaining)-i-1], remaining[i]
		}
		sort.SliceStable(remaining, func(i, j int) bool { return remaining[i].Time > remaining[j].Time })
		for archive := range m.Retentions {
			current, next := extractPoints(remaining, now, m.Retentions[archive].MaxRetention())
			remaining = next
			for i := 0; i < len(current)/2; i++ {
				current[i], current[len(current)-i-1] = current[len(current)-i-1], current[i]
			}
			changed := make(map[int]struct{})
			for _, p := range current {
				interval := align(p.Time, m.Retentions[archive].SecondsPerPoint())
				if err := setPoint(b, m, archive, interval, p.Value); err != nil {
					return err
				}
				changed[interval] = struct{}{}
			}
			if err := s.propagate(b, m, archive, changed); err != nil {
				return err
			}
			if len(remaining) == 0 {
				break
			}
		}
	}
	m.Revision++
	encoded, err := marshalMetadata(m)
	if err != nil {
		return err
	}
	if err := b.Set(catalogKey(m.Name), encoded, nil); err != nil {
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("sync update %s: %w", m.Name, err)
	}
	return nil
}

func extractPoints(points []whisper.TimeSeriesPoint, now, retention int) ([]whisper.TimeSeriesPoint, []whisper.TimeSeriesPoint) {
	maxAge := now - retention
	for i, point := range points {
		if point.Time < maxAge {
			return points[:i], points[i:]
		}
	}
	return points, nil
}

func (s *Store) propagate(reader pebble.Reader, m Metadata, start int, changed map[int]struct{}) error {
	// Keep every original interval eligible at each lower archive. A finer
	// rollup can fail XFF while its lower-resolution bucket is already complete.
	original := make([]int, 0, len(changed))
	for timestamp := range changed {
		original = append(original, timestamp)
	}
	sort.Ints(original)
	for archive := start + 1; archive < len(m.Retentions) && len(original) > 0; archive++ {
		seen := make(map[int]struct{})
		propagated := false
		for _, timestamp := range original {
			interval := align(timestamp, m.Retentions[archive].SecondsPerPoint())
			if _, ok := seen[interval]; ok {
				continue
			}
			seen[interval] = struct{}{}
			wrote, err := s.rollup(reader, m, archive, interval)
			if err != nil {
				return err
			}
			if wrote {
				propagated = true
			}
		}
		if !propagated {
			break
		}
	}
	return nil
}

func (s *Store) rollup(reader pebble.Reader, m Metadata, archive, interval int) (bool, error) {
	higher := m.Retentions[archive-1]
	lower := m.Retentions[archive]
	need := lower.SecondsPerPoint() / higher.SecondsPerPoint()
	values := make([]float64, 0, need)
	for t := interval; t < interval+lower.SecondsPerPoint(); t += higher.SecondsPerPoint() {
		value, ok, err := getPoint(reader, m, archive-1, t)
		if err != nil {
			return false, err
		}
		if ok {
			values = append(values, value)
		}
	}
	if float32(len(values))/float32(need) < m.XFilesFactor || len(values) == 0 {
		return false, nil
	}
	if err := setPoint(reader.(*pebble.Batch), m, archive, interval, aggregate(m.AggregationMethod, values)); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) Fetch(ctx context.Context, name string, fromTime, untilTime int) (*Series, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fromTime > untilTime {
		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", fromTime, untilTime)
	}
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	m, err := metadataFrom(snapshot, name)
	if err != nil {
		return nil, err
	}
	now := int(s.now().Unix())
	oldest := now - maxRetention(m.Retentions)
	if fromTime > now || untilTime < oldest {
		return nil, nil
	}
	if fromTime < oldest {
		fromTime = oldest
	}
	if untilTime > now {
		untilTime = now
	}
	archive := targetArchive(m.Retentions, now-fromTime, -1)
	if archive < 0 {
		return nil, nil
	}
	step := m.Retentions[archive].SecondsPerPoint()
	from := interval(fromTime, step)
	until := interval(untilTime, step)
	if from == until {
		hasPoints, err := hasArchivePoint(snapshot, m, archive)
		if err != nil {
			return nil, err
		}
		if hasPoints {
			until += step
		}
	}
	values, err := getRange(snapshot, m, archive, from, until)
	if err != nil {
		return nil, err
	}
	return &Series{Metadata: m, FromTime: from, UntilTime: until, Step: step, Values: values}, nil
}

func hasArchivePoint(reader pebble.Reader, m Metadata, archive int) (bool, error) {
	prefix := pointPrefixArchive(m.ID, m.Generation, archive)
	it, err := reader.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return false, fmt.Errorf("iterate archive: %w", err)
	}
	defer it.Close()
	return it.First() && it.Valid(), it.Error()
}

func (s *Store) Delete(ctx context.Context, name string) error {
	return s.delete(ctx, name, nil)
}

func (s *Store) delete(ctx context.Context, name string, expected *Metadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock := s.lockMetric(name)
	defer unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.metadata(name)
	if err != nil {
		return err
	}
	if expected != nil && (m.ID != expected.ID || m.Generation != expected.Generation || m.Revision != expected.Revision) {
		return ErrConflict
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Delete(catalogKey(name), nil); err != nil {
		return err
	}
	if m.ID == math.MaxUint64 {
		return errors.New("cannot delete maximum metric ID")
	}
	start, end := pointPrefix(m.ID, 0), pointPrefix(m.ID+1, 0)
	if err := b.DeleteRange(start, end, nil); err != nil {
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("sync delete %s: %w", name, err)
	}
	return nil
}

func (s *Store) Snapshot(ctx context.Context, name string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	snap := s.db.NewSnapshot()
	defer snap.Close()
	m, err := metadataFrom(snap, name)
	if err != nil {
		return Snapshot{}, err
	}
	result := Snapshot{Metadata: m, Archives: make([]Archive, len(m.Retentions))}
	for i, retention := range m.Retentions {
		result.Archives[i].Retention = retention
		prefix := pointPrefixArchive(m.ID, m.Generation, i)
		it, err := snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if err != nil {
			return Snapshot{}, err
		}
		for it.First(); it.Valid(); it.Next() {
			ts, value := decodePoint(it.Value())
			result.Archives[i].Points = append(result.Archives[i].Points, whisper.TimeSeriesPoint{Time: int(ts), Value: value})
		}
		err = it.Close()
		if err != nil {
			return Snapshot{}, err
		}
		sort.Slice(result.Archives[i].Points, func(a, b int) bool { return result.Archives[i].Points[a].Time < result.Archives[i].Points[b].Time })
	}
	return result, nil
}

// Replace atomically publishes a complete archive-preserving metric generation.
func (s *Store) Replace(ctx context.Context, snapshot Snapshot) (Metadata, error) {
	return s.replace(ctx, snapshot, false)
}

func (s *Store) replace(ctx context.Context, snapshot Snapshot, mustAbsent bool) (Metadata, error) {
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := validate(snapshot.Metadata.MetricConfig); err != nil {
		return Metadata{}, err
	}
	if len(snapshot.Archives) != len(snapshot.Metadata.Retentions) {
		return Metadata{}, errors.New("snapshot archive count does not match retentions")
	}
	for i, archive := range snapshot.Archives {
		want := snapshot.Metadata.Retentions[i]
		if archive.Retention.SecondsPerPoint() != want.SecondsPerPoint() || archive.Retention.NumberOfPoints() != want.NumberOfPoints() {
			return Metadata{}, fmt.Errorf("snapshot archive %d retention does not match metadata", i)
		}
	}
	unlock := s.lockMetric(snapshot.Metadata.Name)
	defer unlock()
	return s.replaceLocked(ctx, snapshot, mustAbsent)
}

// Caller holds the metric stripe; catalog publication remains atomic with slots.
func (s *Store) replaceLocked(ctx context.Context, snapshot Snapshot, mustAbsent bool) (Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, err := s.metadata(snapshot.Metadata.Name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Metadata{}, err
	}
	missing := errors.Is(err, ErrNotFound)
	if mustAbsent && !missing {
		return Metadata{}, fmt.Errorf("%w: %s", ErrExists, snapshot.Metadata.Name)
	}
	m := snapshot.Metadata
	if missing {
		id, e := s.nextID()
		if e != nil {
			return Metadata{}, e
		}
		m.ID, m.Generation, m.Revision = id, 1, 1
	} else {
		m.ID, m.Generation, m.Revision = old.ID, old.Generation+1, old.Revision+1
	}
	b := s.db.NewIndexedBatch()
	defer b.Close()
	if !missing {
		if err := b.DeleteRange(pointPrefix(old.ID, old.Generation), pointPrefix(old.ID, old.Generation+1), nil); err != nil {
			return Metadata{}, fmt.Errorf("delete replaced generation: %w", err)
		}
	}
	for i, archive := range snapshot.Archives {
		for _, p := range archive.Points {
			if err := setPoint(b, m, i, align(p.Time, m.Retentions[i].SecondsPerPoint()), p.Value); err != nil {
				return Metadata{}, err
			}
		}
	}
	encoded, err := marshalMetadata(m)
	if err != nil {
		return Metadata{}, err
	}
	if err := b.Set(catalogKey(m.Name), encoded, nil); err != nil {
		return Metadata{}, err
	}
	if missing {
		if err := b.Set(sequenceKey(), uint64Bytes(m.ID), nil); err != nil {
			return Metadata{}, err
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return Metadata{}, fmt.Errorf("sync replace %s: %w", m.Name, err)
	}
	return m, nil
}

func validate(c MetricConfig) error {
	if c.Name == "" {
		return errors.New("metric name is empty")
	}
	if len(c.Retentions) == 0 {
		return errors.New("metric has no retentions")
	}
	for i, r := range c.Retentions {
		if r.SecondsPerPoint() <= 0 || r.NumberOfPoints() <= 0 {
			return fmt.Errorf("invalid retention %d", i)
		}
		if i > 0 && r.SecondsPerPoint() <= c.Retentions[i-1].SecondsPerPoint() {
			return errors.New("retentions must be increasing")
		}
		if i > 0 {
			higher := c.Retentions[i-1]
			if r.SecondsPerPoint()%higher.SecondsPerPoint() != 0 {
				return errors.New("higher precision must evenly divide lower precision")
			}
			if higher.MaxRetention() >= r.MaxRetention() {
				return errors.New("lower precision must cover a larger interval")
			}
			if higher.NumberOfPoints() < r.SecondsPerPoint()/higher.SecondsPerPoint() {
				return errors.New("higher precision has too few points to consolidate")
			}
		}
	}
	if c.AggregationMethod < whisper.Average || c.AggregationMethod > whisper.First {
		return errors.New("unsupported aggregation method")
	}
	if c.XFilesFactor < 0 || c.XFilesFactor > 1 {
		return errors.New("invalid xFilesFactor")
	}
	return nil
}

func cloneConfig(c MetricConfig) MetricConfig {
	c.Retentions = append([]whisper.Retention(nil), c.Retentions...)
	return c
}
func maxRetention(rs []whisper.Retention) int { return rs[len(rs)-1].MaxRetention() }
func align(t, step int) int                   { return t - t%step }
func interval(t, step int) int                { return align(t, step) + step }
func targetArchive(rs []whisper.Retention, diff, target int) int {
	for i, r := range rs {
		if target >= 0 && r.MaxRetention() != target {
			continue
		}
		if target >= 0 || diff <= r.MaxRetention() {
			return i
		}
	}
	return -1
}
func aggregate(method whisper.AggregationMethod, values []float64) float64 {
	switch method {
	case whisper.Average:
		var x float64
		for _, v := range values {
			x += v
		}
		return x / float64(len(values))
	case whisper.Sum:
		var x float64
		for _, v := range values {
			x += v
		}
		return x
	case whisper.First:
		return values[0]
	case whisper.Last:
		return values[len(values)-1]
	case whisper.Max:
		x := values[0]
		for _, v := range values[1:] {
			if v > x {
				x = v
			}
		}
		return x
	case whisper.Min:
		x := values[0]
		for _, v := range values[1:] {
			if v < x {
				x = v
			}
		}
		return x
	}
	panic("unsupported aggregation")
}

func pointPrefix(id, generation uint64) []byte {
	k := make([]byte, 17)
	k[0] = 'p'
	binary.BigEndian.PutUint64(k[1:], id)
	binary.BigEndian.PutUint64(k[9:], generation)
	return k
}
func uint64Bytes(value uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, value)
	return b
}
func pointPrefixArchive(id, generation uint64, archive int) []byte {
	k := make([]byte, 19)
	copy(k, pointPrefix(id, generation))
	binary.BigEndian.PutUint16(k[17:], uint16(archive))
	return k
}
func pointKey(id, generation uint64, archive, slot int) []byte {
	k := make([]byte, 23)
	copy(k, pointPrefixArchive(id, generation, archive))
	binary.BigEndian.PutUint32(k[19:], uint32(slot))
	return k
}
func prefixEnd(k []byte) []byte {
	end := append([]byte(nil), k...)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end
		}
	}
	return nil
}
func setPoint(b *pebble.Batch, m Metadata, archive, timestamp int, value float64) error {
	slot := (timestamp / m.Retentions[archive].SecondsPerPoint()) % m.Retentions[archive].NumberOfPoints()
	return b.Set(pointKey(m.ID, m.Generation, archive, slot), encodePoint(int64(timestamp), value), nil)
}
func getPoint(r pebble.Reader, m Metadata, archive, timestamp int) (float64, bool, error) {
	slot := (timestamp / m.Retentions[archive].SecondsPerPoint()) % m.Retentions[archive].NumberOfPoints()
	v, closer, err := r.Get(pointKey(m.ID, m.Generation, archive, slot))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read point: %w", err)
	}
	defer closer.Close()
	ts, value := decodePoint(v)
	return value, int(ts) == timestamp, nil
}

// getRange uses at most two iterators because circular slots can wrap once.
func getRange(reader pebble.Reader, m Metadata, archive, from, until int) ([]float64, error) {
	retention := m.Retentions[archive]
	step, slots := retention.SecondsPerPoint(), retention.NumberOfPoints()
	values := make([]float64, (until-from)/step)
	for i := range values {
		values[i] = math.NaN()
	}
	remaining := len(values)
	slot := (from / step) % slots
	for remaining > 0 {
		count := remaining
		if count > slots-slot {
			count = slots - slot
		}
		prefix := pointPrefixArchive(m.ID, m.Generation, archive)
		lower := pointKey(m.ID, m.Generation, archive, slot)
		upper := prefixEnd(prefix)
		if slot+count < slots {
			upper = pointKey(m.ID, m.Generation, archive, slot+count)
		}
		it, err := reader.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return nil, fmt.Errorf("iterate points: %w", err)
		}
		for it.First(); it.Valid(); it.Next() {
			timestamp, value := decodePoint(it.Value())
			index := (int(timestamp) - from) / step
			if int(timestamp) >= from && int(timestamp) < until && index >= 0 && index < len(values) {
				values[index] = value
			}
		}
		if err := it.Close(); err != nil {
			return nil, fmt.Errorf("iterate points: %w", err)
		}
		remaining -= count
		slot = 0
	}
	return values, nil
}
func encodePoint(timestamp int64, value float64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, uint64(timestamp))
	binary.BigEndian.PutUint64(b[8:], math.Float64bits(value))
	return b
}
func decodePoint(b []byte) (int64, float64) {
	return int64(binary.BigEndian.Uint64(b)), math.Float64frombits(binary.BigEndian.Uint64(b[8:]))
}

// ExportWSP writes a standard classic Whisper file without mutating the store.
func (s *Store) ExportWSP(ctx context.Context, name, path string) error {
	snapshot, err := s.Snapshot(ctx, name)
	if err != nil {
		return err
	}
	return s.ExportSnapshot(ctx, snapshot, path)
}

// ExportSnapshot exports exactly the generation and revision captured by Snapshot.
func (s *Store) ExportSnapshot(ctx context.Context, snapshot Snapshot, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validate(snapshot.Metadata.MetricConfig); err != nil {
		return err
	}
	if len(snapshot.Archives) != len(snapshot.Metadata.Retentions) {
		return errors.New("snapshot archive count does not match retentions")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create export directory: %w", err)
	}
	w, err := whisper.Create(path, whisper.NewRetentionsNoPointer(snapshot.Metadata.Retentions), snapshot.Metadata.AggregationMethod, snapshot.Metadata.XFilesFactor)
	if err != nil {
		return fmt.Errorf("create export: %w", err)
	}
	for i, a := range snapshot.Archives {
		if err := w.ReplaceArchivePoints(i, a.Points); err != nil {
			_ = w.Close()
			return fmt.Errorf("write archive %d: %w", i, err)
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close export: %w", err)
	}
	return nil
}

// ImportWSP reads a quiesced source and atomically publishes all archives.
func (s *Store) ImportWSP(ctx context.Context, name, path string, replace bool) (Metadata, error) {
	snapshot, err := readWSP(ctx, name, path)
	if err != nil {
		return Metadata{}, err
	}
	return s.replace(ctx, snapshot, !replace)
}

func readWSP(ctx context.Context, name, path string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	snapshotPath, cleanup, err := whisper.OfflineMergeOutOfOrderSnapshot(path)
	if err != nil {
		return Snapshot{}, err
	}
	defer cleanup()
	readOnly := os.O_RDONLY
	w, err := whisper.OpenWithOptions(snapshotPath, &whisper.Options{OpenFileFlag: &readOnly})
	if err != nil {
		return Snapshot{}, fmt.Errorf("open import: %w", err)
	}
	defer w.Close()
	cfg := MetricConfig{Name: name, Retentions: w.Retentions(), AggregationMethod: w.AggregationMethod(), XFilesFactor: w.XFilesFactor()}
	if err := validate(cfg); err != nil {
		return Snapshot{}, err
	}
	if !w.IsCompressed() {
		info, err := os.Stat(snapshotPath)
		if err != nil {
			return Snapshot{}, fmt.Errorf("stat import: %w", err)
		}
		// Check declared capacity before ArchivePoints allocates its buffers.
		if int64(w.Size()) > info.Size() {
			return Snapshot{}, errors.New("import archives exceed file size")
		}
	}
	snapshot := Snapshot{Metadata: Metadata{MetricConfig: cfg}, Archives: make([]Archive, len(cfg.Retentions))}
	for i, r := range cfg.Retentions {
		points, err := w.ArchivePoints(i)
		if err != nil {
			return Snapshot{}, fmt.Errorf("snapshot archive %d: %w", i, err)
		}
		snapshot.Archives[i] = Archive{Retention: r, Points: points}
	}
	return snapshot, nil
}

func (a Archive) PointsToPointers() []*whisper.TimeSeriesPoint {
	p := make([]*whisper.TimeSeriesPoint, len(a.Points))
	for i := range a.Points {
		p[i] = &a.Points[i]
	}
	return p
}

type persistedRetention struct {
	SecondsPerPoint int
	NumberOfPoints  int
}
type persistedMetadata struct {
	Name              string
	Retentions        []persistedRetention
	AggregationMethod whisper.AggregationMethod
	XFilesFactor      float32
	ID                uint64
	Generation        uint64
	Revision          uint64
}

func marshalMetadata(m Metadata) ([]byte, error) {
	p := persistedMetadata{Name: m.Name, AggregationMethod: m.AggregationMethod, XFilesFactor: m.XFilesFactor, ID: m.ID, Generation: m.Generation, Revision: m.Revision, Retentions: make([]persistedRetention, len(m.Retentions))}
	for i, r := range m.Retentions {
		p.Retentions[i] = persistedRetention{r.SecondsPerPoint(), r.NumberOfPoints()}
	}
	return json.Marshal(p)
}
func unmarshalMetadata(value []byte) (Metadata, error) {
	var p persistedMetadata
	if err := json.Unmarshal(value, &p); err != nil {
		return Metadata{}, fmt.Errorf("decode catalog: %w", err)
	}
	m := Metadata{MetricConfig: MetricConfig{Name: p.Name, AggregationMethod: p.AggregationMethod, XFilesFactor: p.XFilesFactor, Retentions: make([]whisper.Retention, len(p.Retentions))}, ID: p.ID, Generation: p.Generation, Revision: p.Revision}
	for i, r := range p.Retentions {
		m.Retentions[i] = whisper.NewRetention(r.SecondsPerPoint, r.NumberOfPoints)
	}
	return m, validate(m.MetricConfig)
}

var _ io.Closer = (*Store)(nil)

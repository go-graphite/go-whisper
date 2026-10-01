package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	whisper "github.com/go-graphite/go-whisper"
)

// ListPage reads at most limit entries from the catalog, exclusive of after.
// A caller scanning multiple pages may see concurrent catalog creations/deletions.
func (s *Store) ListPage(ctx context.Context, prefix, after string, limit int) ([]Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 10000 {
		return nil, errors.New("catalog page limit must be 1..10000")
	}
	lower := catalogKey(prefix)
	if after != "" && strings.Compare(after, prefix) >= 0 {
		lower = append(catalogKey(after), 0)
	}
	upper := prefixEnd(catalogKey(prefix))
	if strings.Compare(string(lower), string(upper)) >= 0 {
		return []Metadata{}, nil
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("open catalog page: %w", err)
	}
	defer it.Close()
	result := make([]Metadata, 0, limit)
	for it.First(); it.Valid() && len(result) < limit; it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m, err := unmarshalMetadata(it.Value())
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("read catalog page: %w", err)
	}
	return result, nil
}

// DeleteIfUnchanged prevents a rebalance snapshot from deleting later writes.
func (s *Store) DeleteIfUnchanged(ctx context.Context, name string, expected Metadata) error {
	return s.delete(ctx, name, &expected)
}

// FillWSP atomically fills empty slots, preserving every existing destination
// value. Incompatible policies are rejected rather than silently resampled.
func (s *Store) FillWSP(ctx context.Context, name, path string) (Metadata, error) {
	source, err := readWSP(ctx, name, path)
	if err != nil {
		return Metadata{}, err
	}
	if err := validate(source.Metadata.MetricConfig); err != nil {
		return Metadata{}, err
	}
	unlock := s.lockMetric(name)
	defer unlock()
	dest, err := s.Snapshot(ctx, name)
	if errors.Is(err, ErrNotFound) {
		return s.replaceLocked(ctx, source, true)
	}
	if err != nil {
		return Metadata{}, err
	}
	if !samePolicy(source.Metadata.MetricConfig, dest.Metadata.MetricConfig) {
		return Metadata{}, fmt.Errorf("%w: %s", ErrConflict, name)
	}
	for i, a := range source.Archives {
		byTime := make(map[int]float64, len(a.Points)+len(dest.Archives[i].Points))
		for _, p := range a.Points {
			if !math.IsNaN(p.Value) {
				byTime[p.Time] = p.Value
			}
		}
		for _, p := range dest.Archives[i].Points {
			if !math.IsNaN(p.Value) {
				byTime[p.Time] = p.Value
			}
		}
		points := make([]whisper.TimeSeriesPoint, 0, len(byTime))
		for timestamp, value := range byTime {
			points = append(points, whisper.TimeSeriesPoint{Time: timestamp, Value: value})
		}
		sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
		dest.Archives[i].Points = points
	}
	return s.replaceLocked(ctx, dest, false)
}

func samePolicy(a, b MetricConfig) bool {
	if a.AggregationMethod != b.AggregationMethod || a.XFilesFactor != b.XFilesFactor || len(a.Retentions) != len(b.Retentions) {
		return false
	}
	for i, r := range a.Retentions {
		if r.SecondsPerPoint() != b.Retentions[i].SecondsPerPoint() || r.NumberOfPoints() != b.Retentions[i].NumberOfPoints() {
			return false
		}
	}
	return true
}

package config

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// ByteSize is a byte count that accepts human-friendly YAML values such as
// 64MiB, 1MB, 512KiB, 4096, or 5_000_000. Binary (KiB/MiB/GiB) and decimal
// (KB/MB/GB) units are both supported; a bare "K/M/G" is binary.
type ByteSize int64

var sizeUnits = []struct {
	suffix string
	mult   int64
}{
	// Longest suffixes first so "MiB" is not matched as "B".
	{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"tib", 1 << 40},
	{"kb", 1e3}, {"mb", 1e6}, {"gb", 1e9}, {"tb", 1e12},
	{"k", 1 << 10}, {"m", 1 << 20}, {"g", 1 << 30}, {"t", 1 << 40},
	{"b", 1},
}

// ParseByteSize parses a human-friendly size.
func ParseByteSize(s string) (ByteSize, error) {
	orig := s
	s = strings.ToLower(strings.TrimSpace(strings.ReplaceAll(s, "_", "")))
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	mult := int64(1)
	for _, u := range sizeUnits {
		if strings.HasSuffix(s, u.suffix) {
			mult = u.mult
			s = strings.TrimSpace(strings.TrimSuffix(s, u.suffix))
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q", orig)
	}
	if n > 0 && mult > (1<<63-1)/n {
		return 0, fmt.Errorf("size %q overflows", orig)
	}
	return ByteSize(n * mult), nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: size must be a scalar", n.Line)
	}
	v, err := ParseByteSize(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*b = v
	return nil
}

// Int64 returns the size as int64.
func (b ByteSize) Int64() int64 { return int64(b) }

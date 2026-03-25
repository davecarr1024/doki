package replicationlog

import (
	"testing"
)

func TestNew(t *testing.T) {
	l := New(10)
	if l.Len() != 0 {
		t.Fatalf("expected empty log, got len=%d", l.Len())
	}
	if l.OldestVersion() != 0 {
		t.Fatalf("expected oldest=0, got %d", l.OldestVersion())
	}
}

func TestAppendAndSince(t *testing.T) {
	l := New(10)
	for i := uint64(1); i <= 5; i++ {
		l.Append(Entry{Term: 1, Version: i, Op: "put", Key: "k", Value: "v"})
	}
	if l.Len() != 5 {
		t.Fatalf("expected len=5, got %d", l.Len())
	}

	entries, ok := l.Since(0)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries since 0, got %d", len(entries))
	}
	if entries[0].Version != 1 || entries[4].Version != 5 {
		t.Fatalf("unexpected versions: %v", entries)
	}
}

func TestSinceMidRange(t *testing.T) {
	l := New(10)
	for i := uint64(1); i <= 10; i++ {
		l.Append(Entry{Term: 1, Version: i, Op: "put", Key: "k"})
	}
	entries, ok := l.Since(7)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries since 7, got %d: %v", len(entries), entries)
	}
	if entries[0].Version != 8 || entries[2].Version != 10 {
		t.Fatalf("unexpected entries: %v", entries)
	}
}

func TestSinceAlreadyUpToDate(t *testing.T) {
	l := New(10)
	for i := uint64(1); i <= 5; i++ {
		l.Append(Entry{Term: 1, Version: i, Op: "put", Key: "k"})
	}
	entries, ok := l.Since(5)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries since 5, got %d", len(entries))
	}
}

func TestSinceEmptyLog(t *testing.T) {
	l := New(10)
	entries, ok := l.Since(0)
	if !ok {
		t.Fatal("expected ok=true for empty log")
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

func TestEviction(t *testing.T) {
	l := New(5)
	for i := uint64(1); i <= 7; i++ {
		l.Append(Entry{Term: 1, Version: i, Op: "put", Key: "k"})
	}
	if l.Len() != 5 {
		t.Fatalf("expected len=5 after eviction, got %d", l.Len())
	}
	if l.OldestVersion() != 3 {
		t.Fatalf("expected oldest=3 after evicting 1,2; got %d", l.OldestVersion())
	}
}

func TestSinceGap(t *testing.T) {
	l := New(5)
	for i := uint64(1); i <= 7; i++ {
		l.Append(Entry{Term: 1, Version: i, Op: "put", Key: "k"})
	}
	// Log now has versions 3-7. sinceVersion=1 implies we need entry at version 2, which is gone.
	_, ok := l.Since(1)
	if ok {
		t.Fatal("expected ok=false when gap exists")
	}
}

func TestSinceExactOldestBoundary(t *testing.T) {
	l := New(5)
	for i := uint64(1); i <= 7; i++ {
		l.Append(Entry{Term: 1, Version: i, Op: "put", Key: "k"})
	}
	// Log has 3-7. sinceVersion=2 → need entry at 3, which is present (oldest=3 ≤ 2+1=3).
	entries, ok := l.Since(2)
	if !ok {
		t.Fatal("expected ok=true at exact oldest boundary")
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries since 2, got %d", len(entries))
	}
}

func TestSinceJustBeyondOldest(t *testing.T) {
	l := New(5)
	for i := uint64(1); i <= 7; i++ {
		l.Append(Entry{Term: 1, Version: i, Op: "put", Key: "k"})
	}
	// Log has 3-7. sinceVersion=3 → entries 4-7.
	entries, ok := l.Since(3)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(entries) != 4 {
		t.Fatalf("expected 4 entries since 3, got %d", len(entries))
	}
}

func TestOldestVersionEmpty(t *testing.T) {
	l := New(10)
	if v := l.OldestVersion(); v != 0 {
		t.Fatalf("expected 0 for empty log, got %d", v)
	}
}

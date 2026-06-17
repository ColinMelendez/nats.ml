package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestKeyValueMarkerTTLSupported(t *testing.T) {
	tests := []struct {
		name      string
		version   string
		supported bool
		valid     bool
	}{
		{name: "before introduction", version: "2.10.22", supported: false, valid: true},
		{name: "first supported release", version: "2.11.0", supported: true, valid: true},
		{name: "later release", version: "2.14.6", supported: true, valid: true},
		{name: "future major", version: "3.0.0", supported: true, valid: true},
		{name: "release candidate", version: "2.11.0-RC.1", supported: true, valid: true},
		{name: "malformed", version: "development", valid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			supported, err := keyValueMarkerTTLSupported(test.version)
			if (err == nil) != test.valid {
				t.Fatalf("validity was %t, expected %t (error: %v)", err == nil, test.valid, err)
			}
			if err == nil && supported != test.supported {
				t.Fatalf("support was %t, expected %t", supported, test.supported)
			}
		})
	}
}

func TestPublishReadyFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "ready")
	if err := publishReadyFile(path, "marker-ttl\n"); err != nil {
		t.Fatalf("publish ready file: %v", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read ready file: %v", err)
	}
	if string(content) != "marker-ttl\n" {
		t.Fatalf("ready content was %q, expected %q", string(content), "marker-ttl\n")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("list ready directory: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "ready" {
		t.Fatalf("ready directory contained %v, expected only the published file", entries)
	}
}

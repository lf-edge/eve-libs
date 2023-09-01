// Copyright (c) 2023 Zededa, Inc.
// SPDX-License-Identifier: Apache-2.0

package persistcache

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type persistCache struct {
	sync.Mutex
	cache map[string]string
	root  string
}

const FILE_MASK = 0755

type InvalidKeyError struct{}

func (e *InvalidKeyError) Error() string {
	return "Key is invalid"
}

// Load values from cache or creates path if there's none
func Load(path string) (*persistCache, error) {
	pc := &persistCache{}
	pc.root = path
	pc.cache = make(map[string]string)

	if _, err := os.Stat(pc.root); os.IsNotExist(err) {
		os.MkdirAll(pc.root, FILE_MASK)
		return pc, nil
	}

	err := filepath.WalkDir(pc.root, func(path string, di fs.DirEntry, err error) error {
		// We skip all directories
		if di.IsDir() {
			return nil
		}

		val, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		pc.cache[di.Name()] = string(val)

		return nil
	})
	return pc, err
}

// Get value from cache
func (pc *persistCache) Get(key string) (string, bool) {
	pc.Lock()
	defer pc.Unlock()

	val, ok := pc.cache[key]
	return val, ok
}

// Create or update value in in-memory cache and filesystem
func (pc *persistCache) Put(key string, val string) (string, error) {
	pc.Lock()
	defer pc.Unlock()

	if !isValidKey(key) {
		return "", &InvalidKeyError{}
	}

	pc.cache[key] = val

	// save file
	filepath := filepath.Join(pc.root, key)
	f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, FILE_MASK)
	if err != nil {
		return "", err
	}
	f.WriteString(val)
	if err := f.Close(); err != nil {
		return "", err
	}

	return filepath, nil
}

// Remove element from cache and filesystem
func (pc *persistCache) Delete(key string) error {
	pc.Lock()
	defer pc.Unlock()

	delete(pc.cache, key)
	return os.Remove(filepath.Join(pc.root, key))
}

func isValidKey(key string) bool {
	// in case of key being ../../../../../../etc/passwd
	if strings.Contains(key, "/") {
		return false
	}

	return true
}

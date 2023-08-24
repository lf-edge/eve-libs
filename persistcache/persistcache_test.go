// Copyright (c) 2023 Zededa, Inc.
// SPDX-License-Identifier: Apache-2.0

package persistcache_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lf-edge/eve-libs/persistcache"
	"gotest.tools/assert"
)

type persistObj struct {
	Name string
	Val  string
}

func TestPut(test *testing.T) {
	path, _ := os.Getwd()
	persistCacheFolder := filepath.Join(path, "testPersist/")
	defer os.RemoveAll(persistCacheFolder)

	pc, _ := persistcache.Load(persistCacheFolder)

	// Create
	obj1 := persistObj{
		Name: "object1",
		Val:  "123",
	}
	pc.Put(obj1.Name, obj1.Val)
	expected, _ := pc.Get(obj1.Name)
	assert.Equal(test, obj1.Val, expected)

	// Update
	newVal := "320"
	pc.Put(obj1.Name, newVal)
	expected, _ = pc.Get(obj1.Name)
	assert.Equal(test, newVal, expected)
}

func TestDelete(test *testing.T) {
	path, _ := os.Getwd()
	persistCacheFolder := filepath.Join(path, "testPersist/")
	defer os.RemoveAll(persistCacheFolder)

	obj1 := persistObj{
		Name: "object1",
		Val:  "123",
	}

	pc, _ := persistcache.Load(persistCacheFolder)
	pc.Put(obj1.Name, obj1.Val)

	val, _ := pc.Get(obj1.Name)
	assert.Equal(test, obj1.Val, val)

	pc.Delete(obj1.Name)

	val, _ = pc.Get(obj1.Name)
	assert.Equal(test, "", val)
}

func TestLoad(test *testing.T) {
	path, _ := os.Getwd()
	persistCacheFolder := filepath.Join(path, "testPersist/")
	defer os.RemoveAll(persistCacheFolder)

	obj1 := persistObj{
		Name: "object1",
		Val:  "123",
	}
	obj2 := persistObj{
		Name: "object2",
		Val:  "bazinga",
	}

	pc, _ := persistcache.Load(persistCacheFolder)

	pc.Put(obj1.Name, obj1.Val)
	pc.Put(obj2.Name, obj2.Val)

	val, _ := pc.Get(obj1.Name)
	assert.Equal(test, obj1.Val, val)
	val, _ = pc.Get(obj2.Name)
	assert.Equal(test, obj2.Val, val)

	// Check that we can access same files from new object
	pc2, _ := persistcache.Load(persistCacheFolder)

	val, _ = pc2.Get(obj1.Name)
	assert.Equal(test, obj1.Val, val)

	val, _ = pc2.Get(obj2.Name)
	assert.Equal(test, obj2.Val, val)
}

package cashing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddNode(t *testing.T) {
	hashRing := NewHashRing(3, nil)

	hashRing.AddNode("node1")
	hashRing.AddNode("node2")
	hashRing.AddNode("node3")

	assert.Equal(t, 3*hashRing.replicas, len(hashRing.nodes), "Expected %d nodes in the hash ring, got %d", 3*hashRing.replicas, len(hashRing.nodes))
}

func TestRemoveNode(t *testing.T) {
	hashRing := NewHashRing(3, nil)

	hashRing.AddNode("node1")
	hashRing.AddNode("node2")
	hashRing.AddNode("node3")

	hashRing.RemoveNode("node2")

	assert.Equal(t, 2*hashRing.replicas, len(hashRing.nodes), "Expected %d nodes in the hash ring after removing node2, got %d", 2*hashRing.replicas, len(hashRing.nodes))

	_, ok := hashRing.nodesMap[hashRing.hash("node2-0")]
	assert.False(t, ok, "Node2 replica 0 should be removed from nodesMap")
}

func TestGetNode(t *testing.T) {
	hashRing := NewHashRing(3, nil)

	hashRing.AddNode("node1")
	hashRing.AddNode("node2")
	hashRing.AddNode("node3")

	key := "test"
	node := hashRing.GetNode(key)

	assert.NotEmpty(t, node, "GetNode should return a non-empty node for key '%s'", key)

	hashRing.RemoveNode("node2")

	newNode := hashRing.GetNode(key)

	assert.NotEmpty(t, newNode, "GetNode should return a non-empty node for key '%s' after removing node2", key)
	assert.NotEqual(t, node, newNode, "Nodes before and after removing node2 should be different")
}

func TestChangeHashFunction(t *testing.T) {
	numTimesHashCalled := 0

	customHashFunction := func(key hashKey) uint32 {
		hash := uint32(0)
		for _, char := range key {
			hash += uint32(char)
		}
		numTimesHashCalled++
		return hash
	}

	hashRing := NewHashRing(1, customHashFunction)

	hashRing.AddNode("node1")
	hashRing.AddNode("node2")
	hashRing.AddNode("node3")

	key := "foobar"
	node := hashRing.GetNode(key)

	assert.NotEmpty(t, node, "GetNode should return a non-empty node for key '%s'", key)

	hashRing.RemoveNode("node1")

	newNode := hashRing.GetNode(key)

	assert.NotEmpty(t, newNode, "GetNode should return a non-empty node for key '%s' after removing node2", key)
	assert.NotEqual(t, node, newNode, "Nodes before and after removing node2 should be different")

	assert.Equal(t, 6, numTimesHashCalled, "The hash function should have been called 4 times")
}

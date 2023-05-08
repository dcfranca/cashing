package cashing

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

type hashFunction func(string) uint32

type HashRing[T fmt.Stringer] struct {
	nodes    []uint32
	nodesMap map[uint32]*T
	replicas int
	mutex    sync.RWMutex
	hash     hashFunction
}

// Instantiate a new Hash Ring
// You can define your own hash function or
// pass nil to use the default one: sha1
func NewHashRing[T fmt.Stringer](replicas int, hash hashFunction) *HashRing[T] {
	if hash == nil {
		hash = defaultHashFunction
	}

	return &HashRing[T]{
		nodes:    make([]uint32, 0),
		nodesMap: make(map[uint32]*T),
		replicas: replicas,
		mutex:    sync.RWMutex{},
		hash:     hash,
	}
}

func defaultHashFunction(key string) uint32 {
	hash := sha1.New()
	hash.Write([]byte(key))
	hashBytes := hash.Sum(nil)
	return binary.BigEndian.Uint32(hashBytes)
}

// Add a node to the hash ring
// the node parameter is just a string identifier, i.e: node1
func (hr *HashRing[T]) AddNode(node T) {
	hr.mutex.Lock()
	defer hr.mutex.Unlock()

	for i := 0; i < hr.replicas; i++ {
		replicaKey := fmt.Sprintf("%s-%d", node, i)
		hash := hr.hash(replicaKey)
		hr.nodes = append(hr.nodes, hash)
		hr.nodesMap[hash] = &node
	}
	sort.Slice(hr.nodes, func(i, j int) bool { return hr.nodes[i] < hr.nodes[j] })
}

// Remove a node from the hash ring
// Returns an error in case the node is not found
// The node parameter is the node string identifier
func (hr *HashRing[T]) RemoveNode(node string) error {
	hr.mutex.Lock()
	defer hr.mutex.Unlock()
	deleted := false

	for i := 0; i < hr.replicas; i++ {
		replicaKey := fmt.Sprintf("%s-%d", node, i)
		hash := hr.hash(replicaKey)
		index := sort.Search(len(hr.nodes), func(i int) bool { return hr.nodes[i] >= hash })
		if index < len(hr.nodes) && hr.nodes[index] == hash {
			// Remove the node found
			hr.nodes = append(hr.nodes[:index], hr.nodes[index+1:]...)
			delete(hr.nodesMap, hash)
			deleted = true
		}
	}

	if !deleted {
		return fmt.Errorf("node not found")
	}

	return nil
}

// Get the node associated with the key
func (hr *HashRing[T]) GetNode(key string) *T {
	hr.mutex.RLock()
	defer hr.mutex.RUnlock()

	if len(hr.nodes) == 0 {
		return nil
	}

	hash := hr.hash(key)
	// The first node that has a key > than the key being searched for is the node with the key stored
	index := sort.Search(len(hr.nodes), func(i int) bool { return hr.nodes[i] >= hash })

	// As it is a ring, if it is bigger than the number of nodes, it should return to 0
	if index == len(hr.nodes) {
		index = 0
	}

	return hr.nodesMap[hr.nodes[index]]
}

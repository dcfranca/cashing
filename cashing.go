package cashing

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

type hashKey string
type hashFunction func(hashKey) uint32

type HashRing struct {
	nodes    []uint32
	nodesMap map[uint32]string
	replicas int
	mutex    sync.RWMutex
	hash     hashFunction
}

// Instantiate a new Hash Ring
// You can define your own hash function or
// pass nil to use the default one: sha1
func NewHashRing(replicas int, hash hashFunction) *HashRing {
	if hash == nil {
		hash = defaultHashFunction
	}

	return &HashRing{
		nodes:    make([]uint32, 0),
		nodesMap: make(map[uint32]string),
		replicas: replicas,
		mutex:    sync.RWMutex{},
		hash:     hash,
	}
}

func defaultHashFunction(key hashKey) uint32 {
	hash := sha1.New()
	hash.Write([]byte(key))
	hashBytes := hash.Sum(nil)
	return binary.BigEndian.Uint32(hashBytes)
}

func (hr *HashRing) AddNode(node string) {
	hr.mutex.Lock()
	defer hr.mutex.Unlock()

	for i := 0; i < hr.replicas; i++ {
		replicaKey := fmt.Sprintf("%s-%d", node, i)
		hash := hr.hash(hashKey(replicaKey))
		hr.nodes = append(hr.nodes, hash)
		hr.nodesMap[hash] = node
	}
	sort.Slice(hr.nodes, func(i, j int) bool { return hr.nodes[i] < hr.nodes[j] })
}

func (hr *HashRing) RemoveNode(node string) {
	hr.mutex.Lock()
	defer hr.mutex.Unlock()

	for i := 0; i < hr.replicas; i++ {
		replicaKey := fmt.Sprintf("%s-%d", node, i)
		hash := hr.hash(hashKey(replicaKey))
		index := sort.Search(len(hr.nodes), func(i int) bool { return hr.nodes[i] >= hash })
		if index < len(hr.nodes) && hr.nodes[index] == hash {
			// Remove the node found
			hr.nodes = append(hr.nodes[:index], hr.nodes[index+1:]...)
			delete(hr.nodesMap, hash)
		}
	}
}

func (hr *HashRing) GetNode(key string) string {
	hr.mutex.RLock()
	defer hr.mutex.RUnlock()

	if len(hr.nodes) == 0 {
		return ""
	}

	hash := hr.hash(hashKey(key))
	// The first node that has a key > than the key being searched for is the node with the key stored
	index := sort.Search(len(hr.nodes), func(i int) bool { return hr.nodes[i] >= hash })

	// As it is a ring, if it is bigger than the number of nodes, it should return to 0
	if index == len(hr.nodes) {
		index = 0
	}

	return hr.nodesMap[hr.nodes[index]]
}

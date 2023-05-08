# cashing

Consistency hashing library for Go applications

# Installation

```
go get -u github.com/dcfranca/cashing
```

# Usage

Create a new hash ring with 3 replicas (virtual nodes) and the default hash function
The NodeType must implement the Stringer interface

```
	hashRing := NewHashRing[NodeType](3, nil)
```

Add 3 nodes to it

```
	hashRing.AddNode("node1")
	hashRing.AddNode("node2")
	hashRing.AddNode("node3")
```

Get the node to store your data, it returns a pointer to your NodeType

```
	key := "foobar"
	node := hashRing.GetNode(key)
```

Remove a node

```
    err := hashRing.RemoveNode("node2")
    if err != nil {
        fmt.Println("node not found")
    }
```

Use a custom hash function

```
	customHashFunction := func(key string) uint32 {
		hash := uint32(0)
		for _, char := range key {
			hash += uint32(char)
		}
		numTimesHashCalled++
		return hash
	}

	hashRing := NewHashRing[NodeType](1, customHashFunction)
```

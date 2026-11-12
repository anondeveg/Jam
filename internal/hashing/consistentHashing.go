package hashing

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"maps"
	"slices"
	"strconv"
	"sync"
)

const RINGSIZE uint64 = 1 << 32

type ConsistentHashing struct {
	rwmutex         sync.RWMutex
	VnodesMuliplier int
	Owner           map[uint64]string
	ring            []uint64 // sorted list of keys to run BS on.
}

func NewConsistentHashing(vnodesMultiplier int) *ConsistentHashing {
	return &ConsistentHashing{
		VnodesMuliplier: vnodesMultiplier,
		Owner:           make(map[uint64]string),
		ring:            make([]uint64, 0),
	}
}

func (c *ConsistentHashing) sortMap() {
	keys := slices.Collect(maps.Keys(c.Owner))
	slices.Sort(keys)
	c.ring = keys
}

func (c *ConsistentHashing) hash(key string) uint64 {
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(sum[:8]) % uint64(RINGSIZE)
}

func (c *ConsistentHashing) NewServer(server string) {
	c.rwmutex.Lock()
	defer c.rwmutex.Unlock()

	for i := 0; i < c.VnodesMuliplier; i++ {
		pos := c.hash(server + "$VSHN " + strconv.Itoa(i))
		_, taken := c.Owner[pos]
		if !taken {
			c.Owner[pos] = server
		}

	}
	c.sortMap()

}

func (c *ConsistentHashing) findServerBS(x uint64) int {
	idx, _ := slices.BinarySearch(c.ring, x)

	if idx == len(c.ring) {
		return 0 // wrap around to the first node.
	}

	return idx
}

func (c *ConsistentHashing) GetServer(key string) (string, error) {

	keyHash := c.hash(key)
	c.rwmutex.RLock()
	defer c.rwmutex.RUnlock()

	if len(c.ring) == 0 {
		return "", errors.New("No servers.")
	}

	server := c.findServerBS(keyHash)
	return c.Owner[c.ring[server]], nil

}

func (c *ConsistentHashing) RemoveServer(server string) {
	c.rwmutex.Lock()
	defer c.rwmutex.Unlock()

	for i := 0; i < c.VnodesMuliplier; i++ {
		pos := c.hash(server + "$VSHN " + strconv.Itoa(i))
		if c.Owner[pos] == server {
			delete(c.Owner, pos)
		}
	}
	c.sortMap()
}

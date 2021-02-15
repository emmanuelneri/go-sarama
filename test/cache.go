package test

import "sync"

type Cache struct {
	values []string
	mu     sync.Mutex
}

func NewCache() *Cache {
	return &Cache{
		values: make([]string, 0),
	}
}

func (c *Cache) Add(value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values = append(c.values, value)
}

func (c *Cache) Values() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values
}

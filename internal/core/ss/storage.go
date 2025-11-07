package ss

import (
	"slices"
	"sync"
	"varvaDB/internal/domain"
)

type storage struct {
	levels []*level
	mu     *sync.RWMutex
}

func NewStorage() *storage {
	zero := NewLevel(0)
	return &storage{
		levels: []*level{zero},
		mu:     &sync.RWMutex{},
	}
}

type level struct {
	mu     *sync.RWMutex
	index  int
	tables []*domain.SSTable
}

func NewLevel(index int) *level {
	tables := make([]*domain.SSTable, 0)

	return &level{
		tables: tables,
		mu:     &sync.RWMutex{},
		index:  index,
	}
}

func (s *storage) AppendInLevel(table *domain.SSTable, index int) {

	s.mu.Lock()
	if len(s.levels) <= index {

		lastIndex := s.levels[len(s.levels)-1].index
		for len(s.levels) <= index {
			level := NewLevel(lastIndex)
			s.levels = append(s.levels, level)
			lastIndex++
		}

	}
	s.mu.Unlock()

	s.levels[index].mu.Lock()
	s.levels[index].tables = append(s.levels[index].tables, table)
	slices.SortFunc(s.levels[index].tables, func(a, b *domain.SSTable) int {
		if a.GetTimestamp() > b.GetTimestamp() {
			return 1 // a новее b → a должен быть после
		}
		if a.GetTimestamp() < b.GetTimestamp() {
			return -1 // a старее b → a должен быть первым
		}
		return 0 // равны
	})

	s.levels[index].mu.Unlock()
}

// func (s *storage) DeleteTable(table *domain.SSTable) bool {
// 	version := table.GetVersion()

// 	if len(s.levels) <= int(version) {
// 		return false
// 	}

// 	level := s.levels[int(version)]

// 	level.mu.Lock()
// 	for i := range level.tables {
// 		if table.GetID() == level.tables[i].GetID() {
// 			newTables := make([]*domain.SSTable, 0, len(level.tables)-1)

// 			newTables = append(newTables, level.tables[:i]...)
// 			if i < len(level.tables)-1 {
// 				newTables = append(newTables, level.tables[i+1:]...)
// 			}

// 			level.tables = newTables
// 			break
// 		}
// 	}
// 	level.mu.Unlock()

// 	return true
// }

// func (s *storage) DeleteTables(table *domain.SSTable) (bool){
// 	version := table.GetVersion()

// 	if len(s.levels) <= int(version) {
// 		return false
// 	}

// 	level := s.levels[int(version)]

// 	level.mu.Lock()
// 	for i := range level.tables {
// 		if table.Getid() == level.tables[i].Getid() {
// 			newTables := make([]*domain.SSTable, 0, len(level.tables)-1)

// 			newTables = append(newTables, level.tables[:i]...)
// 			if i < len(level.tables)-1 {
// 				newTables = append(newTables, level.tables[i+1:]...)
// 			}

// 			level.tables = newTables
// 			break
// 		}
// 	}
// 	level.mu.Unlock()

// 	return true
// }

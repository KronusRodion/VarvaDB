package domain

import "sync"

type SSTable struct {
	id         uint64
	version    uint16
	createdAt  int64
	dataStart  uint64
	indexStart uint64
	bloomStart uint64
	bloomSize  uint32
	minKey     []byte
	maxKey     []byte
	filter     []byte
	flushMutex *sync.RWMutex
}

type Iterator interface {
    Next() bool      
    Record() *Record 
    Error() error
	Close() error
}

func NewSSTable(id uint64, version uint16, createdAt int64, dataStart, indexStart, bloomStart uint64, bloomSize uint32, minKey, maxKey, filter []byte) *SSTable {
	return &SSTable{
		id:         id,
		version:    version,
		createdAt:  createdAt,
		dataStart:  dataStart,
		indexStart: indexStart,
		bloomStart: bloomStart,
		bloomSize:  bloomSize,
		minKey:     minKey,
		maxKey:     maxKey,
		filter:     filter,
		flushMutex: &sync.RWMutex{},
	}
}

func (s *SSTable) GetTimestamp() int64 {
	return s.createdAt
}

func (s *SSTable) GetBloomMaskInfo() (uint64, uint32) {
	return s.bloomStart, s.bloomSize
}

func (s *SSTable) GetID() uint64 {
	return s.id
}

func (s *SSTable) GetMaxKey() []byte {
	return s.maxKey
}

func (s *SSTable) GetMinKey() []byte {
	return s.minKey
}

func (s *SSTable) GetIndexOffset() uint64 {
	return s.indexStart
}

func (s *SSTable) GetbloomStart() uint64 {
	return s.bloomStart
}

func (s *SSTable) GetDataStart() uint64 {
	return s.dataStart
}

func (s *SSTable) GetVersion() uint16 {
	return s.version
}

func (s *SSTable) TryToLock() bool {
	return s.flushMutex.TryLock()
}

func (s *SSTable) Unlock() {
	s.flushMutex.Unlock()
}



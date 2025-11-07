package engine

import (
	"context"
	"sync"
	"varvaDB/config"
	memtable "varvaDB/internal/core/memTable"
	"varvaDB/internal/core/ss"
	wal "varvaDB/internal/core/wal"
	"varvaDB/internal/domain"
)

type LSMTree struct {
	memTable   *memtable.Table //активная memTable
	immemTable *memtable.Table // иммутабельная memTable для сброса
	cfg        *config.Config
	mu         *sync.RWMutex

	walManager        *wal.Manager // Для хранения данных об несохраненных транзакциях
	sstManager *ss.Manager
}

type LSMCore struct {
	*LSMTree
}

var _ Core = &LSMCore{}

func NewLSM(cfg *config.Config) (*LSMTree, error) {
	walChan := make(chan uint64)
	walManager := wal.NewManager(&cfg.WAL, walChan)

	tree := LSMTree{
		immemTable: nil,
		mu:         &sync.RWMutex{},
		walManager:        walManager,
		cfg:        cfg,
	}

	return &tree, nil
}

// BuildCore - строит ядро проекта на базе LSM дерева
func (l *LSMTree) BuildCore(ctx context.Context) (Core, error) {
	err := l.walManager.Start(ctx)
	if err != nil {
		return nil, err
	}
	
	walRecords := l.walManager.GetActiveWal().GetRecords()
	deleteWalChan := l.walManager.GetDeleteChan()
	walChangeFunc := l.walManager.ChangeWAL

	memtable, recordsChan := memtable.New(walChangeFunc, l.cfg.Memtable.MaxSize)
	memtable.Restore(walRecords)

	sstManager := ss.NewManager(l.cfg, recordsChan, deleteWalChan)

	l.memTable = memtable
	l.sstManager = sstManager

	return &LSMCore{l}, nil
}

func (l *LSMCore) Start(ctx context.Context) {
	l.sstManager.Start(ctx)
}

func (l *LSMCore) Info() {
	// log.Println("Информативная сводка...")
	// log.Println("Размер memtable", l.memTable.Size())
}

func (l *LSMCore) Put(key, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	record := domain.NewRecord(key, value, domain.OP_PUT)
	err := l.walManager.WriteRecord(record)
	if err != nil {
		return err
	}
	l.memTable.Put(key, value)

	return nil
}

func (l *LSMCore) Delete(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.memTable.Delete(key)
}

func (l *LSMCore) Get(key string) ([]byte, bool) {
	l.mu.RLock()
	value, ok := l.memTable.Get(key)
	l.mu.RUnlock()
	if !ok {
		return l.sstManager.Find([]byte(key))
	}
	return value, true
}

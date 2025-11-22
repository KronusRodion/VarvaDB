package ss

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"varvaDB/config"
	"varvaDB/internal/domain"
)

type Manager struct {
	sstChan   chan *domain.SSTable
	workdir   string
	SSTPrefix string
	storage   *storage
	tableSize int
	levelSize int
	mu        *sync.RWMutex
	saver     *Saver
}

func NewManager(cfg *config.Config, recordsChan chan *domain.SSMessage, deleteWalChan chan<- uint64) *Manager {
	storage := NewStorage(cfg.SSTPrefix, cfg.SSTWorkdir, cfg.SSTManager.FlushSize, cfg)

	// NewSaver принимает записи из канала и
	saver, ch := newSaver(cfg, recordsChan, deleteWalChan)

	return &Manager{
		saver:     saver,
		sstChan:   ch,
		workdir:   cfg.SSTWorkdir,
		SSTPrefix: cfg.SSTPrefix,
		storage:   storage,
		tableSize: cfg.Compactor.TableSize,
		levelSize: cfg.Compactor.LevelSize,
		mu:        &sync.RWMutex{},
	}
}

func (m *Manager) Size() int {
	size := 0
	m.mu.Lock()
	for _, level := range m.storage.levels {
		size += len(level.tables)
	}
	m.mu.Unlock()
	return size
}

func (m *Manager) Start(ctx context.Context) error {

	if m.workdir == "" {
		return errors.New("workdir is not set")
	}
	err := m.CheckSST(ctx)
	if err != nil {
		return err
	}
	go m.HandleSSTChan(ctx)

	m.saver.Start(ctx)
	log.Println("sstManager запущен.")
	return nil
}

func (m *Manager) HandleSSTChan(ctx context.Context) {

	loop:
	for {
		select {
		case sst := <-m.sstChan:
			m.mu.Lock()
			m.storage.AppendInLevel(ctx, sst, int(sst.GetVersion()))
			m.mu.Unlock()

		case <-ctx.Done():
			log.Println("Остановка sstManager по контексту...")
			break loop
		}
	}
}

func (m *Manager) CheckSST(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := os.MkdirAll(m.workdir, 0755); err != nil {
		return err
	}

	log.Println("begin dir read: ", m.workdir)
	files, err := os.ReadDir(m.workdir)
	if err != nil {
		return err
	}
	for _, fileEntry := range files {
		log.Println("file Entry ", fileEntry.Name())
		if !strings.HasPrefix(fileEntry.Name(), m.SSTPrefix) {
			continue
		}
		path := filepath.Join(m.workdir, fileEntry.Name())
		file, err := os.Open(path)
		if err != nil {
			log.Println("Ошибка чтения файлы в директории с SS таблицами: ", err)
			continue
		}
		defer file.Close()

		sst, err := m.ReadSST(file)
		if err != nil {
			log.Println("Ошибка чтения файлы в директории с SS таблицами: ", err)
			continue
		}
		m.storage.AppendInLevel(ctx, sst, int(sst.GetVersion()))
	}

	log.Printf("Найдено %d ss таблиц", len(m.storage.levels[0].tables))

	return nil
}

func (m *Manager) ReadSST(file io.ReadSeeker) (*domain.SSTable, error) {

	var sstID uint64
    err := binary.Read(file, binary.LittleEndian, &sstID)
    if err != nil {
        return nil, fmt.Errorf("failed to read version: %w", err)
    }

	log.Println("vers")
	var version uint16
	err = binary.Read(file, binary.LittleEndian, &version)
	if err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}
	log.Println("crea")
	var createdAt int64
	err = binary.Read(file, binary.LittleEndian, &createdAt)
	if err != nil {
		return nil, fmt.Errorf("failed to read creation time: %w", err)
	}

	log.Println("data_start")
	// Получаем позицию начала данных
	dataStart, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to get current position: %w", err)
	}
	dataStart += 4 // +4 для dataLen

	log.Println("dataLen")
	// Читаем длину данных
	var dataLen uint32
	err = binary.Read(file, binary.LittleEndian, &dataLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read data length: %w", err)
	}
	log.Println("lastRecordOffset")

	// Пропускаем данные (нам нужен только первый и последний ключ)
	_, err = file.Seek(int64(dataLen), io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to skip data: %w", err)
	}

	// Читаем offset последней записи
	var lastRecordOffset uint64
	err = binary.Read(file, binary.LittleEndian, &lastRecordOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read last record offset: %w", err)
	}

	// Теперь читаем minKey и maxKey используя сохраненные offsets
	var minKey, maxKey []byte

	// Читаем minKey из начала данных
	_, err = file.Seek(int64(dataStart), io.SeekStart) // Переходим к началу данных
	if err != nil {
		return nil, fmt.Errorf("failed to seek to data start: %w", err)
	}
	log.Println("minmax")
	// Пропускаем operation и timestamp первой записи
	_, err = file.Seek(1+8, io.SeekCurrent) // operation(4) + timestamp(8)
	if err != nil {
		return nil, fmt.Errorf("failed to skip first operation/timestamp: %w", err)
	}

	// Читаем minKey
	var minKeyLen uint32
	err = binary.Read(file, binary.LittleEndian, &minKeyLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read min key length: %w", err)
	}
	log.Println("minmax1")
	minKey = make([]byte, minKeyLen)
	_, err = io.ReadFull(file, minKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read min key: %w", err)
	}

	// Читаем maxKey из последней записи
	_, err = file.Seek(int64(lastRecordOffset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to last record: %w", err)
	}

	// Пропускаем operation и timestamp последней записи
	_, err = file.Seek(1+8, io.SeekCurrent) // operation(4) + timestamp(8)
	if err != nil {
		return nil, fmt.Errorf("failed to skip last operation/timestamp: %w", err)
	}
	log.Println("minmax2")
	// Читаем maxKey
	var maxKeyLen uint32
	err = binary.Read(file, binary.LittleEndian, &maxKeyLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read max key length: %w", err)
	}
	maxKey = make([]byte, maxKeyLen)
	_, err = io.ReadFull(file, maxKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read max key: %w", err)
	}

	// Возвращаемся к чтению остальных секций
	// Позиция после lastRecordOffset - это начало индекса
	indexStart := dataStart + int64(dataLen) + 8 // dataStart + dataLen + lastRecordOffset(8)
	log.Println("indexStart")
	_, err = file.Seek(indexStart, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to index start: %w", err)
	}

	// Читаем длину индекса
	var indexLen uint32
	err = binary.Read(file, binary.LittleEndian, &indexLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read index length: %w", err)
	}

	// Пропускаем индекс
	_, err = file.Seek(int64(indexLen), io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to skip index: %w", err)
	}
	log.Println("bloomStart")
	// Позиция начала bloom фильтра
	bloomStart := indexStart + int64(indexLen) + 4 // +4 для indexLen

	// Определяем размер bloom фильтра (читаем до конца файла)
	fileEnd, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to get file size: %w", err)
	}

	bloomSize := uint32(fileEnd - bloomStart)

	// Читаем bloom фильтр
	_, err = file.Seek(bloomStart, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to bloom filter: %w", err)
	}
	log.Println("bloomMask")
	bloomMask := make([]byte, bloomSize)
	_, err = io.ReadFull(file, bloomMask)
	if err != nil {
		return nil, fmt.Errorf("failed to read bloom filter: %w", err)
	}

	sst := domain.NewSSTable(
		sstID,
		version,
		createdAt,
		uint64(dataStart),
		uint64(indexStart),
		uint64(bloomStart),
		bloomSize,
		minKey,
		maxKey,
		bloomMask,
	)

	return sst, nil
}

func (m *Manager) Find(key []byte) ([]byte, bool) {
	for i := range m.storage.levels {
		level := m.storage.levels[i]
		level.mu.RLock()
		defer level.mu.RUnlock()

		for j := range level.tables {
			
			table := level.tables[j]
			if bytes.Compare(key, table.GetMinKey()) < 0 || bytes.Compare(key, table.GetMaxKey()) > 0 {
				continue
			}

			tableID := table.GetID()
			fileName := fmt.Sprintf("%s_%d", m.SSTPrefix, tableID)
			path := filepath.Join(m.workdir, fileName)

			file, err := os.Open(path)
			if err != nil {
				log.Println("Произошла ошибка при открытии ss таблицы: ", err)
				continue
			}
			defer file.Close()

			bloomOffset, bloomSize := table.GetBloomMaskInfo()
			_, err = file.Seek(int64(bloomOffset), io.SeekStart)
			if err != nil {
				log.Println("Ошибка при перемещении к bloom фильтру: ", err)
				continue
			}

			mask := make([]byte, bloomSize)
			_, err = io.ReadFull(file, mask)
			if err != nil {
				log.Println("Ошибка при чтении bloom маски: ", err)
				continue
			}

			if !m.saver.bloomFilter.Test(key, mask) {
				log.Println("Ключа нет в ", tableID)
				continue
			}

			// Поиск через индекс
			value, found := m.findViaIndex(table, file, key)
			if found && value == nil {
				return nil, false
			} else if found && value != nil {
				return value, true
			}

		}
	}

	return nil, false
}

func (m *Manager) findViaIndex(table *domain.SSTable, file *os.File, key []byte) ([]byte, bool) {

	indexStart := table.GetIndexOffset()
	_, err := file.Seek(int64(indexStart), io.SeekStart)
	if err != nil {
		log.Println("Ошибка при перемещении к индексу: ", err)
		return nil, false
	}


	var indexLen uint32
	err = binary.Read(file, binary.LittleEndian, &indexLen)
	if err != nil {
		log.Println("Ошибка при чтении длины индекса: ", err)
		return nil, false
	}

	var prevIndex *domain.Index
	var startOffset, endOffset uint64

	// Читаем индексные записи
	bytesRead := uint32(0)
	for bytesRead < indexLen {
		index, err := m.ReadIndex(file)
		if err != nil {
			break
		}

		cmp := bytes.Compare(key, index.Key)

		if cmp == 0 {
			log.Println("Ключ найден прямо в индексе")
			_, err := file.Seek(int64(index.Offset), io.SeekStart)
			if err != nil {
				log.Println("Ошибка при перемещении к записи по индексу: ", err)
				return nil, false
			}

			record, err := domain.ReadRecord(file)
			if err != nil {
				log.Println(err)
				return nil, false
			}
			return record.GetValue(), true

		} else if cmp < 0 {
			log.Println("Ключ меньше чем индекс")
			// Ключ меньше текущего индексного ключа
			if prevIndex != nil {
				// Ищем в диапазоне от предыдущего индекса до текущего
				startOffset = prevIndex.Offset
				endOffset = index.Offset
				return m.readRecordUntil(file, key, startOffset, endOffset)
			} else {
				log.Println("Ключ больше чем индекс")
				// Ищем от начала данных до первого индекса
				startOffset = table.GetDataStart()
				endOffset = index.Offset
				return m.readRecordUntil(file, key, startOffset, endOffset)
			}
		} else {
			// Ключ больше текущего индексного ключа - сохраняем и продолжаем
			prevIndex = &index
		}

		bytesRead += 4 + uint32(len(index.Key)) + 8
	}

	// Если дошли до конца индекса и не нашли
	if prevIndex != nil {
		// Ищем в диапазоне от последнего индекса до конца данных
		startOffset = prevIndex.Offset
		endOffset, _ = table.GetBloomMaskInfo()
		return m.readRecordUntil(file, key, startOffset, endOffset)
	}

	// Если индексов нет вообще - ищем во всех данных
	startOffset = table.GetDataStart()
	endOffset, _ = table.GetBloomMaskInfo()
	return m.readRecordUntil(file, key, startOffset, endOffset)
}

func (m *Manager) readRecordUntil(file *os.File, key []byte, startOffset, endOffset uint64) ([]byte, bool) {
	// Перемещаемся к началу диапазона
	_, err := file.Seek(int64(startOffset), io.SeekStart)
	if err != nil {
		log.Println("Ошибка при перемещении к началу диапазона: ", err)
		return nil, false
	}

	currentOffset := startOffset

	// Линейный поиск в указанном диапазоне
	for currentOffset < endOffset {

		record, err := domain.ReadRecord(file)
		if err != nil {
			log.Println(err)
			break
		}
		cmp := bytes.Compare(key, record.GetKey())

		if cmp == 0 {
			// Нашли ключ
			if record.GetOperation() == domain.OP_PUT {
				return record.GetValue(), true
			} else if record.GetOperation() == domain.OP_DELETE {
				return nil, true // Ключ удален
			}
		} else if cmp < 0 {
			// Прошли нужный ключ (ключи отсортированы по возрастанию)
			// Больше нет смысла искать
			break
		}
		// Обновляем текущую позицию
		currentPos, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println(err)
			break
		}
		currentOffset = uint64(currentPos)
	}

	return nil, false
}

func (m *Manager) ReadIndex(reader *os.File) (domain.Index, error) {
	var keyLen uint32
	err := binary.Read(reader, binary.LittleEndian, &keyLen)
	if err != nil {
		return domain.Index{}, err
	}

	key := make([]byte, keyLen)
	_, err = io.ReadFull(reader, key)
	if err != nil {
		return domain.Index{}, err
	}

	var offset uint64
	err = binary.Read(reader, binary.LittleEndian, &offset)
	if err != nil {
		return domain.Index{}, err
	}

	return domain.Index{
		Key:    key,
		Offset: offset,
	}, nil
}

package ss

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
	"varvaDB/config"
	"varvaDB/internal/bloom"
	"varvaDB/internal/domain"
	"varvaDB/internal/iterators"
	"varvaDB/pkg/utils"
)

type storage struct {
	levels 				[]*level
	mu     				*sync.RWMutex
	SSTPrefix			string
	workDir				string
	maxTablesInLevel	int
	bloomFilter			*bloom.BloomFilter
	clearTime			int
}

func NewStorage(SSTPrefix, workDir string, maxTablesInLevel int, cfg *config.Config) *storage {
	zero := NewLevel(0)
	filter := bloom.NewFilter(cfg.SSTManager.Bloom.ExpectedElements, cfg.SSTManager.Bloom.FalsePositiveRate)
	
	return &storage{
		levels: []*level{zero},
		mu:     &sync.RWMutex{},
		SSTPrefix: SSTPrefix,
		workDir: workDir,
		maxTablesInLevel: maxTablesInLevel,
		bloomFilter: filter,
		clearTime:	cfg.SSTManager.ClearTime,
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

func (s *storage) Start(ctx context.Context) {

	ticker := time.NewTicker(time.Duration(s.clearTime) * time.Second)
	log.Printf("Хранилище запущено. Очистка будет производиться каждые %d секунд.", s.clearTime)
	go func(){
		for {
		select {
		case <- ctx.Done():
			log.Println("Остановка хранилища...")
			return
		case <- ticker.C:
			log.Println("Очистка хранилища...")
			s.Clear()
		}
	}
	}()
}
func (s storage) Clear() (error) {
	actualTables := make(map[string]struct{})

	for _, level := range s.levels {
		level.mu.Lock()
		for _, v := range level.tables {
			tableId := v.GetID()
			key := fmt.Sprintf("%s_%d", s.SSTPrefix, tableId)
			actualTables[key] = struct{}{}
		}
		level.mu.Unlock()
	}

	files, err := os.ReadDir(s.workDir)
	if err != nil {
		return err
	}
	for _, fileEntry := range files {
		log.Println("file entry ", fileEntry.Name())
		if _, ok := actualTables[fileEntry.Name()]; !ok {
			log.Println("Удаляем таблицу ", fileEntry.Name())
			path := filepath.Join(s.workDir, fileEntry.Name())
			err := os.Remove(path)
			if err != nil {
				log.Println("Ошибка удаления старой таблицы: ", err)
			}
		}

	}

	return nil
}

func (s *storage) AppendInLevel(ctx context.Context, table *domain.SSTable, index int) {
	log.Printf("Добавляем новую таблицу в %d уровень", index)
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
	log.Println("s.levels[index].tables ", len(s.levels[index].tables))
	log.Println("s.maxTablesInLevel ", s.maxTablesInLevel)
	
	if len(s.levels[index].tables) % s.maxTablesInLevel == 0 {
		s.levels[index].mu.Unlock()
		log.Println("Запускаем flush")
		go s.Flush(ctx, s.levels[index])
	} else {
		s.levels[index].mu.Unlock()
	}
	
}

func (s *storage) DeleteTable(table *domain.SSTable) bool {
	version := table.GetVersion()

	if len(s.levels) <= int(version) {
		return false
	}
	level := s.levels[int(version)]

	level.mu.Lock()
	for i := range level.tables {
		if table.GetID() == level.tables[i].GetID() {
			newTables := make([]*domain.SSTable, 0, len(level.tables)-1)

			newTables = append(newTables, level.tables[:i]...)
			if i < len(level.tables)-1 {
				newTables = append(newTables, level.tables[i+1:]...)
			}

			level.tables = newTables
			break
		}
	}
	level.mu.Unlock()

	return true
}

func (s *storage) DeleteTables(levelIndex int, oldTables []*domain.SSTable) (bool) {
	
	if len(s.levels[levelIndex].tables) < len(oldTables) {
		return false
	}

	newTableSlice := make([]*domain.SSTable, 0, len(s.levels[levelIndex].tables) - len(oldTables)+1)

	level := s.levels[levelIndex]

	for i := range level.tables {
		table := level.tables[i]

		isOld := false

		for  j:= range oldTables {
			oldTable := oldTables[j]
			if oldTable.GetID() == table.GetID() {
				isOld = true
				break
			}
		}

		if !isOld {
			newTableSlice = append(newTableSlice, table)
		}
	}

	level.tables = newTableSlice

	return true
}


func (s *storage) Flush(ctx context.Context, level *level) {
	length := s.maxTablesInLevel

	oldTables := make([]*domain.SSTable, 0, length)
	level.mu.Lock()
	for i := len(level.tables) -1 ;i >= 0; i-- {
		if locked := level.tables[i].TryToLock(); locked {
			oldTables = append(oldTables, level.tables[i])
			if len(oldTables) == length {
				break
			}
		}
	}
	level.mu.Unlock()
	log.Println("Добавлено таблиц на удаление - ", len(oldTables))
	
	if len(oldTables) != length {
		return
	}
	log.Println("Начинаем merge")
	table, err := s.MergeTables(ctx, oldTables, level.index+1)
	if err != nil {
		return
	}

	level.mu.Lock()
	level.tables = append(level.tables, table)
	deleted := s.DeleteTables(level.index, oldTables)
	if !deleted {
		log.Println("Старые таблицы не удалены!")
	}
	
	level.mu.Unlock()
	log.Println("Закончили append")
}


func (s *storage) MergeTables(ctx context.Context, tables []*domain.SSTable, version int) (*domain.SSTable, error) {
	id := utils.GenerateID()
	saveChan := make(chan *domain.Record, 1000)
	outChan := make(chan SaverResponse)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go s.SaveInNewSST(ctx, saveChan, id, uint16(version), outChan)

	iterator_arr := make([]domain.Iterator, 0, len(tables))
	for i := range tables {
		table := tables[i]
		name := fmt.Sprintf("%s_%d", s.SSTPrefix, table.GetID())
		path := filepath.Join(s.workDir, name)
		iterator, err := iterators.NewTableIterator(table, path)
		if err != nil {
			log.Println(err)
			continue
		}
		defer iterator.Close()

		iterator_arr = append(iterator_arr, iterator)
	}
	if len(iterator_arr) < 1 {
		return nil, fmt.Errorf("iterator error")
	}

	heap := NewHeap(len(iterator_arr))
	err := heap.Init(iterator_arr)
	if err != nil {
		return nil, err
	}

	for heap.Len() > 0 {
		minRecord := heap.Pop()
		log.Println("minRecord - ", minRecord)
		saveChan <- minRecord.Record

		// Достаем все копии записи
		for heap.Len() > 0 && minRecord == heap.MinElement() {
			_ = heap.Pop()
		}

		if minRecord.Iterator.Next() {
			newRecord := minRecord.Iterator.Record()
			if err = minRecord.Iterator.Error(); err != nil {
				return nil, err
			}
			heap.Push(&HeapItem{
				Record: newRecord,
				Iterator: minRecord.Iterator,
			})
		}
	}

	close(saveChan)


	select {
	case mergedTable := <-outChan:
		return mergedTable.table, mergedTable.err
	case <- ctx.Done():
		return nil, fmt.Errorf("timeout error")
	}
	
}



type SaverResponse struct {
	table 		*domain.SSTable
	err 		error
}

func (s *storage) SaveInNewSST(ctx context.Context, data chan *domain.Record, id uint64, version uint16, out chan<- SaverResponse) {
	
	fileName := fmt.Sprintf("%s_%d", s.SSTPrefix, id)
	filepath := filepath.Join(s.workDir, fileName)
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE, 0755)

	errFunc := func(err error) {
		select {
		case out<- SaverResponse{nil, err}:
		case <-ctx.Done():
			return
		}
	}

	if err != nil {
		errFunc(err)
		return
	}
	defer func() {
		file.Close()
		// Если ошибка не nil выполняем rollback файла
		if err != nil {
			os.Remove(filepath)
		}

	}()

	err = binary.Write(file, binary.LittleEndian, id)
	if err != nil {
		errFunc(err)
		return
	}

	err = binary.Write(file, binary.LittleEndian, version)
	if err != nil {
		errFunc(err)
		return
	}

	createdAt := time.Now().Unix()
	err = binary.Write(file, binary.LittleEndian, createdAt)
	if err != nil {
		errFunc(err)
		return
	}

	dataStart := 8 + 2 + 8 + 4 // длина id, версии и времени в байтах + int32(в которыых лежит длина записей)

	var dataBuf bytes.Buffer

	indexSlice := make([]*domain.Index, 0)
	var lastRecordOffset uint64

	mask := s.bloomFilter.GenerateMask([][]byte(nil))

	firstRecord := <-data
	log.Println("Пришла первая запись - ", firstRecord)
	{
		record := firstRecord
		key := record.GetKey()
		value := record.GetValue()
		operation := record.GetOperation()
		timestamp := record.GetTimestamp()
		lastRecordOffset = uint64(dataStart) + uint64(dataBuf.Len())
		binary.Write(&dataBuf, binary.LittleEndian, operation)
		binary.Write(&dataBuf, binary.LittleEndian, timestamp)

		// Записываем длину ключа и сам ключ
		binary.Write(&dataBuf, binary.LittleEndian, uint32(len(key)))
		binary.Write(&dataBuf, binary.LittleEndian, key)

		// Записываем длину значения и само значение
		binary.Write(&dataBuf, binary.LittleEndian, uint32(len(value)))
		binary.Write(&dataBuf, binary.LittleEndian, value)
		s.bloomFilter.AddKeyToMask(key, mask)
	}

	

	var lastKey []byte
	i := 1
	for record := range data {
		key := record.GetKey()
		value := record.GetValue()
		operation := record.GetOperation()
		timestamp := record.GetTimestamp()

		if i%20 == 0 {
			offset := uint64(dataStart) + uint64(dataBuf.Len())
			indexSlice = append(indexSlice, &domain.Index{Key: key, Offset: offset})
		}
		lastRecordOffset = uint64(dataStart) + uint64(dataBuf.Len())
		binary.Write(&dataBuf, binary.LittleEndian, operation)
		binary.Write(&dataBuf, binary.LittleEndian, timestamp)

		// Записываем длину ключа и сам ключ
		binary.Write(&dataBuf, binary.LittleEndian, uint32(len(key)))
		binary.Write(&dataBuf, binary.LittleEndian, key)

		// Записываем длину значения и само значение
		binary.Write(&dataBuf, binary.LittleEndian, uint32(len(value)))
		binary.Write(&dataBuf, binary.LittleEndian, value)
		lastKey = key
		s.bloomFilter.AddKeyToMask(key, mask)
		i++
	}


	// Вносим длину записей в файл
	err = binary.Write(file, binary.LittleEndian, uint32(dataBuf.Len()))
	if err != nil {
		errFunc(err)
		return
	}

	// Вносим записи в файл
	err = binary.Write(file, binary.LittleEndian, dataBuf.Bytes())
	if err != nil {
		errFunc(err)
		return
	}

	// Записываем offset последней записи
	err = binary.Write(file, binary.LittleEndian, lastRecordOffset)
	if err != nil {
		errFunc(err)
		return
	}

	indexStart := dataStart + dataBuf.Len() + 8 // +8 для оффсета последнего ключа

	var indexBuf bytes.Buffer
	for i := range indexSlice {
		record := indexSlice[i]
		key := record.Key
		offset := record.Offset

		// Записываем длину ключа и сам ключ
		binary.Write(&indexBuf, binary.LittleEndian, uint32(len(key)))
		binary.Write(&indexBuf, binary.LittleEndian, key)

		// Записываем офсет в формате uint64
		binary.Write(&indexBuf, binary.LittleEndian, offset)
	}

	// Вносим длину индексов в файл
	err = binary.Write(file, binary.LittleEndian, uint32(indexBuf.Len()))
	if err != nil {
		errFunc(err)
		return
	}

	// Вносим индексы в файл
	err = binary.Write(file, binary.LittleEndian, indexBuf.Bytes())
	if err != nil {
		errFunc(err)
		return
	}

	bloomStart := indexStart + indexBuf.Len() + 4
	bloomSize := len(mask)

	_, err = file.Write(mask)
	if err != nil {
		errFunc(err)
		return
	}

	sst := domain.NewSSTable(id, version, createdAt, uint64(dataStart), uint64(indexStart), uint64(bloomStart), uint32(bloomSize), firstRecord.GetKey(), lastKey, mask)

	select {
		case out<- SaverResponse{sst, nil}:
			close(out)
		case <-ctx.Done():
		}
}



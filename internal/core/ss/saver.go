package ss

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	"varvaDB/config"
	"varvaDB/internal/bloom"
	"varvaDB/internal/domain"
)

type Saver struct {
	workDir     	string // Путь к директории с ss таблицами
	SSTPrefix   	string
	dataChan    	chan *domain.SSMessage // Канал со слайсом отсортированных по ключу записей
	sstChan     	chan *domain.SSTable
	bloomFilter 	*bloom.BloomFilter
	deleteWalChan	chan<- uint64	
}

// newSaver - возвращает объект saver и канал, куда будут отправляться сформированные таблицы
func newSaver(cfg *config.Config, dataChan chan *domain.SSMessage,deleteWalChan chan<- uint64) (*Saver, chan *domain.SSTable) {
	sstChan := make(chan *domain.SSTable)
	filter := bloom.NewFilter(cfg.SSTManager.Bloom.ExpectedElements, cfg.SSTManager.Bloom.FalsePositiveRate)

	manager := &Saver{
		workDir:     cfg.SSTWorkdir,
		SSTPrefix:   cfg.SSTPrefix,
		dataChan:    dataChan,
		sstChan:     sstChan,
		bloomFilter: filter,
		deleteWalChan:deleteWalChan,
	}

	return manager, sstChan
}

func (s *Saver) Start(ctx context.Context) {
	go s.handleMemChan(ctx)
	log.Println("SST Manager запущен.")
}

func (s *Saver) handleMemChan(ctx context.Context) {
log.Println("Начинаем держать канал с записями")
loop:
	for {
		select {
		case data := <-s.dataChan:
			go func(data *domain.SSMessage){
				sst, err := s.saveInSST(data.GetRecords(), data.GetWalID())
			if err != nil {
				log.Println("Ошибка сохранения данных в SST: ", err)
				return
			}
			s.sstChan <- sst
			s.deleteWalChan <- data.GetWalID()
			}(data)

		case <-ctx.Done():
			log.Println("Остановка SST Manager по контексту...")
			break loop
		}
	}
}

func (s *Saver) saveInSST(data []*domain.Record, id uint64) (*domain.SSTable, error) {
	log.Println("data - ", len(data))
	if len(data) < 2 {
		return nil, fmt.Errorf("gain too small data array")
	}
	minKey := data[0]
	maxKey := data[len(data)-1]

	version := uint16(0)
	fileName := fmt.Sprintf("%s_%d", s.SSTPrefix, id)
	log.Println("fileName - ", fileName)
	filepath := filepath.Join(s.workDir, fileName)
	log.Println("filepath - ", filepath)
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	err = binary.Write(file, binary.LittleEndian, version)
	if err != nil {
		return nil, err
	}

	createdAt := time.Now().Unix()
	err = binary.Write(file, binary.LittleEndian, createdAt)
	if err != nil {
		return nil, err
	}

	dataStart := 8 + 2 + 8 + 4 // длина id, версии и времени в байтах + int32(в которыых лежит длина записей)

	var dataBuf bytes.Buffer
	indexRecordsCount := len(data) / 20

	indexSlice := make([]*domain.Index, 0, indexRecordsCount)
	keys := make([][]byte, len(data))
	var lastRecordOffset uint64

	for i := range data {
		record := data[i]
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
		keys[i] = key
	}

	// Вносим длину записей в файл
	err = binary.Write(file, binary.LittleEndian, uint32(dataBuf.Len()))
	if err != nil {
		return nil, err
	}

	// Вносим записи в файл
	err = binary.Write(file, binary.LittleEndian, dataBuf.Bytes())
	if err != nil {
		return nil, err
	}

	// Записываем offset последней записи
	err = binary.Write(file, binary.LittleEndian, lastRecordOffset)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	// Вносим индексы в файл
	err = binary.Write(file, binary.LittleEndian, indexBuf.Bytes())
	if err != nil {
		return nil, err
	}

	bloomStart := indexStart + indexBuf.Len() + 4

	bloomMask := s.bloomFilter.GenerateMask(keys)
	bloomSize := len(bloomMask)

	_, err = file.Write(bloomMask)
	if err != nil {
		return nil, err
	}

	sst := domain.NewSSTable(id, version, createdAt, uint64(dataStart), uint64(indexStart), uint64(bloomStart), uint32(bloomSize), minKey.GetKey(), maxKey.GetKey(), bloomMask)

	log.Printf("Пришло %d записей", len(data))
	return sst, nil
}

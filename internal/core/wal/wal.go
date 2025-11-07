package wal

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	"varvaDB/config"
	"varvaDB/internal/domain"
)

type Wal struct {
	id 		  uint64
	file      *os.File
	mu        *sync.Mutex
	filepath  string
	createdAt int64
}

func New(cfg *config.WalConfig) (*Wal, error) {
	
	if err := os.MkdirAll(cfg.WalWorkdir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}
	createdAt := time.Now().Unix()
	rawID := make([]byte, 8)
	rand.Read(rawID)
	id := binary.LittleEndian.Uint64(rawID)

	log.Println("Создаем новый журнал c timestamp ", createdAt)
	walName := fmt.Sprintf("wal_%d.log", id)

	walFilePath := filepath.Join(cfg.WalWorkdir, walName)

	file, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL file: %v", err)
	}

	writer := bufio.NewWriter(file)
	err = binary.Write(writer, binary.LittleEndian, createdAt)
	if err != nil {
		return nil, fmt.Errorf("failed to write createdAt in WAL file: %v", err)
	}

	path := fmt.Sprintf("%s\\%s", cfg.WalWorkdir, file.Name())
	var mu sync.Mutex

	wal := &Wal{
		id:id,
		file:      file,
		filepath:  path,
		mu:        &mu,
		createdAt: createdAt,
	}
	
	return wal, nil
}

func Open(cfg config.WalConfig, walPath string, createdAt int64) (*Wal, error) {

	if err := os.MkdirAll(cfg.WalWorkdir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}


	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL file: %v", err)
	}

	path := fmt.Sprintf("%s\\%s", cfg.WalWorkdir, file.Name())
	var mu sync.Mutex

	wal := &Wal{
		file:      file,
		filepath:  path,
		mu:        &mu,
		createdAt: createdAt,
	}

	return wal, nil
}

func (w *Wal) writeRecord(record *domain.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	key := record.GetKey()
	value := record.GetValue()
	operation := record.GetOperation()
	timestamp := record.GetTimestamp()

	err := binary.Write(w.file, binary.LittleEndian, operation)
	if err != nil {
		return err
	}

	err = binary.Write(w.file, binary.LittleEndian, timestamp)
	if err != nil {
		return err
	}

	err = binary.Write(w.file, binary.LittleEndian, uint32(len(key)))
	if err != nil {
		return err
	}

	_, err = w.file.Write(key)
	if err != nil {
		return err
	}

	err = binary.Write(w.file, binary.LittleEndian, uint32(len(value)))
	if err != nil {
		return err
	}

	_, err = w.file.Write(value)
	if err != nil {
		return err
	}

	return nil
}

func (w *Wal) GetRecords() []*domain.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	oldRecords := make([]*domain.Record, 0, 1000) // Выделяем место для 1000 записей для уменьшения кол-ва аллокаций

	for {
		record, err := domain.ReadRecord( w.file)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Println("Ошибка при чтении записи: ", err)
			continue
		}
		oldRecords = append(oldRecords, record)
	}
	log.Printf("Было найдено %d старых записей", len(oldRecords))
	return oldRecords
}




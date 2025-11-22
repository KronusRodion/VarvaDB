package wal

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"varvaDB/config"
	"varvaDB/internal/domain"
)

type Manager struct {
	activeWal  *Wal
	notifyChan chan uint64 //Канал, куда будут приходить id wal журнала, под удаление

	cfg *config.WalConfig
}

func NewManager(cfg *config.WalConfig, notify chan uint64) *Manager {
	return &Manager{
		cfg:        cfg,
		notifyChan: notify,
	}
}

// GetDeleteChan - возвращает канал, слушая который, менеджер удаляет wal журналы по id
func (m *Manager) GetDeleteChan() chan<- uint64 {
	return m.notifyChan
}

func (m *Manager) ChangeWAL() (uint64, error) {
	log.Println("Выполняем запрос по смене wal")
	id := m.activeWal.id
	wal, err := New(m.cfg)
	if err != nil {
		return 0, nil
	}
	log.Println("Создали новый wal")
	err = m.activeWal.file.Close()
	if err != nil {
		return 0, nil
	}

	m.activeWal = wal
	log.Println("Поменяли wal c timestamp ", wal.createdAt)
	return id, nil
}

func (m *Manager) Start(ctx context.Context) error {
	err := m.findActiveLog()
	if err != nil {
		return err
	}

	go func(){
		for {
			select {
				case <-ctx.Done():
					log.Println("Удаление wal журналов остановлено...")
					return
				case id := <- m.notifyChan:
					go m.DeleteWal(m.cfg, id)
			}
		}
	}()
	log.Println("WAL Manager запущен")
	return nil
}

func (m *Manager) GetActiveWal() *Wal {
	return m.activeWal
}

func (m *Manager) WriteRecord(record *domain.Record) error {

	err :=  m.activeWal.writeRecord(record)
	return err
}

func (m *Manager) findActiveLog() error {
	log.Println("Выполняем поиск активного журнала")
	if err := os.MkdirAll(m.cfg.WalWorkdir, 0755); err != nil {
		return err
	}

	log.Println("begin dir read: ", m.cfg.WalWorkdir)
	files, err := os.ReadDir(m.cfg.WalWorkdir)
	if err != nil {
		return err
	}
	log.Println("len - ", len(files))
	if len(files) < 1 {
		wal, err := New(m.cfg)
		if err != nil {
			return err
		}
		m.activeWal = wal
		return nil
	}

	

	var lastMod int64 // поле, по которому идет сравнение
	var walCreatedAt int64 // дата создания wal, которая пишется при создании журнала
	var filePath string // путь к wal
	for _, fileEntry := range files {
		if !strings.HasPrefix(fileEntry.Name(), m.cfg.WALPrefix) {
			continue
		}
		path := filepath.Join(m.cfg.WalWorkdir, fileEntry.Name())
		file, err := os.Open(path)
		if err != nil {
			log.Println("Ошибка открытия wal журнала", err)
			continue
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			log.Println("Ошибка открытия инфосводки wal журнала", err)
			continue
		}
		modifyed := info.ModTime()
		if lastMod < modifyed.Unix() {
			

			var createdAt int64
			err := binary.Read(file, binary.LittleEndian, &createdAt)
			if err != nil {
				log.Println("Ошибка чтения времени создания wal: ", err)
			}
			lastMod = modifyed.Unix()
			walCreatedAt = createdAt
			filePath = path
		}
	}

	// В случае, если ни один журнал не прочитан
	if lastMod == 0 {
		wal, err := New(m.cfg)
		if err != nil {
			return err
		}
		m.activeWal = wal
		return nil
	}

	wal, err := Open(*m.cfg, filePath, walCreatedAt)
	if err != nil {
		return err
	}

	m.activeWal = wal
	log.Println("Прочли директории")

	return nil

}

func (m *Manager) DeleteWal(cfg *config.WalConfig, id uint64) ( error) {
	
	walName := fmt.Sprintf("wal_%d.log", id)
	log.Println("Удаляем ", walName)
	walFilePath := filepath.Join(cfg.WalWorkdir, walName)

	err := os.Remove(walFilePath)
	if err != nil {
		return err
	}

	return nil
}

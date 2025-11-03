package memtable

import (
	"bytes"
	"log"
	"sort"
	"time"
	"varvaDB/internal/domain"
)

type Table struct {
	data    			map[string]*domain.Record
	size    			int
	resChan 			chan *domain.SSMessage
	walChangeFunc		func() (uint64, error)
	maxSize				int
}

func New(walChangeFunc func() (uint64, error), maxSize int) (*Table, chan *domain.SSMessage) {
	data := make(map[string]*domain.Record, 100)
	resChan := make(chan *domain.SSMessage)
	return &Table{
		data:    data,
		size:    0,
		resChan: resChan,
		walChangeFunc: walChangeFunc,
		maxSize: maxSize,
	}, resChan
}

func (mt *Table) Put(key, value []byte) {

	oldRecord, ok := mt.data[string(key)]
	if ok {
		mt.size += len(value) - len(oldRecord.GetValue())
		oldRecord.SetValue(value).SetTimestamp(time.Now().Unix()).SetOperation(domain.OP_PUT)

	} else {
		record := domain.NewRecord(key, value, domain.OP_PUT)
		mt.data[string(key)] = record
		mt.size += 1 + 8 + len(key) + len(value)
	}

	if mt.size >= mt.maxSize {
		mt.Flush()
	}
}

func (mt *Table) Get(key string) ([]byte, bool) {
	record, ok := mt.data[key]
	if !ok {
		return nil, false
	}
	log.Println("op ", record.GetOperation())
	if record.GetOperation() == domain.OP_DELETE {
		return nil, false
	}

	log.Printf("Полученное значение: %s", string(record.GetValue()))
    log.Printf("Байты значения: %v", record.GetValue())


	return record.GetValue(), ok
}

func (mt *Table) Delete(key string) {

	if oldValue, ok := mt.data[key]; ok {
		mt.size -= len(oldValue.GetValue())
		oldValue.SetValue(nil).SetOperation(domain.OP_DELETE).SetTimestamp(time.Now().Unix())
		
	} else {
		record := domain.NewRecord([]byte(key), nil, domain.OP_DELETE)
		mt.data[string(key)] = record
		mt.size += 1 + 8 + len(key) + 0
	}
}

func (mt *Table) Size() int {

	log.Println("Количество ключей в мапе ",len(mt.data))
	return mt.size
}

func (mt *Table) GetSortedRecords() []*domain.Record {

	records := make([]*domain.Record, 0, len(mt.data))
	for _, value := range mt.data {
		records = append(records, value)
	}
	sort.Slice(records, func(i, j int) bool {
		a := records[i].GetKey()
		b := records[j].GetKey()
		return bytes.Compare(a, b) < 0
	})

	return records
}

func (mt *Table) Restore(records []*domain.Record) {

	log.Printf("Под восстановление передано %d записей", len(records))
	for i := range records {
		record := records[i]
		mt.data[string(record.GetKey())] = record
		mt.size += 1 + 8 + len(record.GetValue()) + len(record.GetKey())
	}
}


func (mt *Table) Flush() {
	log.Println("Размер memtable превышен")
	walID, err := mt.walChangeFunc()
	if err != nil {
		log.Println("Ошибка создания нового wal")
		return
	}
	
	records := mt.GetSortedRecords()
	log.Println("Отправляем аписи в ss мэнеджер")
	msg := domain.NewSSMessage(records, walID)
	mt.resChan <- msg
	log.Println("Записи отправлены")
	mt.size = 0
	mt.data = make(map[string]*domain.Record, 100)
}
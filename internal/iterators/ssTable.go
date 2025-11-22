package iterators

import (
	"fmt"
	"io"
	"os"
	"varvaDB/internal/domain"
)

type TableIterator struct {
	currentOffset	uint64
	dataEnd 		uint64
	table  			*domain.SSTable
	err 			error
	file 			*os.File
	currentRecord   *domain.Record
}

func (t *TableIterator) Next() bool {
    if t.currentOffset >= t.dataEnd || t.err != nil {
        return false
    }
    
    _, err := t.file.Seek(int64(t.currentOffset), io.SeekStart)
    if err != nil {
        t.err = err
        return false
    }
    
    record, err := domain.ReadRecord(t.file)
    if err != nil {
        t.err = err
        return false
    }
    
    t.currentRecord = record
    
    currentPos, err := t.file.Seek(0, io.SeekCurrent)
    if err != nil {
        t.err = err
        return false
    }
    
    t.currentOffset = uint64(currentPos)
    return true
}

func (t *TableIterator) Close() error {
	return t.file.Close()
}

func (t *TableIterator) Record() *domain.Record {
	return t.currentRecord
}
    

func (t *TableIterator) Error() error {
	return t.err
}


func NewTableIterator(table *domain.SSTable, path string) (domain.Iterator, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	 _, err = file.Seek(int64(table.GetDataStart()), io.SeekStart)
    if err != nil {
        file.Close()
        return nil, fmt.Errorf("failed to seek to data start: %w", err)
    }
	
	return &TableIterator{
		table: table,
		currentOffset: table.GetDataStart(),
		dataEnd: table.GetIndexOffset()-8, // 8 байт перед началом индекса это оффсет на последний элемент
		err: nil,
		file: file,
	}, nil
}
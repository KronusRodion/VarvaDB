package domain

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"
)

type Record struct {
	key       []byte
	value     []byte
	timestamp int64
	operation Operation //0 - put, 1 - delete
}

func NewRecord(key, value []byte, operation Operation) *Record {
	timestamp := time.Now().Unix()
	return &Record{
		key:       key,
		value:     value,
		operation: operation,
		timestamp: timestamp,
	}
}

func NewRecordWithTimestamp(key, value []byte, operation Operation, timestamp int64) *Record {
	return &Record{
		key:       key,
		value:     value,
		operation: operation,
		timestamp: timestamp,
	}
}

func (r *Record) GetKey() []byte {
	return r.key
}

func (r *Record) GetValue() []byte {
	return r.value
}

func (r *Record) GetOperation() Operation {
	return r.operation
}

func (r *Record) GetTimestamp() int64 {
	return r.timestamp
}

func (r *Record) SetValue(value []byte) *Record {
	r.value = value
	return r
}

func (r *Record) SetTimestamp(timestamp int64) *Record {
	r.timestamp = timestamp
	return r
}

func (r *Record) SetOperation(operation Operation) *Record {
	r.operation = operation
	return r
}

func ReadRecord(f *os.File) (*Record, error) {
	log.Println("Начинаем чтение записи")
	var operation byte
	err := binary.Read(f, binary.LittleEndian, &operation)
	if err != nil {
		return nil, err
	}
	log.Println("timestamp")
	var timestamp int64
	err = binary.Read(f, binary.LittleEndian, &timestamp)
	if err != nil {
		return nil, err
	}

	var keyLen int32
	err = binary.Read(f, binary.LittleEndian, &keyLen)
	if err != nil {
		return nil, err
	}

	key := make([]byte, keyLen)
	err = binary.Read(f, binary.LittleEndian, &key)
	if err != nil {
		return nil, err
	}
	var valueLen int32
	err = binary.Read(f, binary.LittleEndian, &valueLen)
	if err != nil {
		return nil, err
	}
	log.Println("valueLen - ", valueLen)
	if valueLen < 0 {
		return nil, fmt.Errorf("invalid value length: %d (hex: %x)", 
			valueLen, uint32(valueLen))
	}
	
	value := make([]byte, valueLen)
	err = binary.Read(f, binary.LittleEndian, &value)
	if err != nil {
		return nil, err
	}

	record := NewRecordWithTimestamp(key, value, Operation(operation), timestamp)


	return record, nil
}
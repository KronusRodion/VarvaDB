package ss

import (
	"bytes"
	"varvaDB/internal/domain"
)

// HeapItem - элемент в куче
type HeapItem struct {
	Record 		*domain.Record
	Iterator 	domain.Iterator
}

// RecordHeap - слайс записей от итераторов, имитирующий бинарное дерево
type RecordHeap []*HeapItem

// NewHeap - возвращает объект кучи
func NewHeap(len int) RecordHeap {
	return make(RecordHeap, 0, len)
}

func (h *RecordHeap) Init(iterators []domain.Iterator) error {
	for i := range iterators {
		iter := iterators[i]
		if iter.Next() {
			record := iter.Record()
			if err := iter.Error(); err != nil {
				return err
			}
			h.Push(
				&HeapItem{
					Record: record,
					Iterator: iter,
				},
			)
		}
	}
	return nil
}

// Len - возвращает длину кучи
func (h RecordHeap) Len() int {
	return len(h)
}
// MinElement - возвращает минимальный элемент кучи
func (h RecordHeap) MinElement() *HeapItem {
	return h[0]
}

// Less - возвращает true, если i-элемент кучи меньше j-элемента (или если ключи равны и i-элемент новее), в ином случае - false 
func (h RecordHeap) Less(i, j int) bool { 
    // Сравниваем ключи байт-лексикографически
    res := bytes.Compare(h[i].Record.GetKey(), h[j].Record.GetKey())
	if res == 0 {
		return h[i].Record.GetTimestamp() < h[j].Record.GetTimestamp()
	}
	return res > 0
}

// Swap - меняет - i, j-элементы местами
func (h RecordHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push - добавляет новый элемент в хип
func (h *RecordHeap) Push(item *HeapItem) {
	*h = append(*h, item)
	i := len(*h) - 1 // Индекс добавленного элемента

	//Теперь нужно обеспечить всплытие элемента в дереве
	// Пока i не станет нулем
	for i > 0 {
		parent := i - 1
		// Проверяем, что новая запись меньше родительской
		if (*h).Less(i, parent) {
			break
		}
		// В ином случае свапаем и продолжаем
		(*h).Swap(i, parent)
		i = parent
	}
}

func (h *RecordHeap) Pop() *HeapItem {
	item := (*h)[0]
	
	if len(*h) > 1 {
		(*h) = (*h)[1:]
	} else {
		(*h) = NewHeap(1)
	}
	return item
}
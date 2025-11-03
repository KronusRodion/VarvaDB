package bloom

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// BloomFilterConfig конфигурация фильтра Блума
type BloomFilterConfig struct {
	Size     uint64   // Размер битового массива в битах
	HashFunc []string // Используемые хэш-функции (пока используем FNV с разными солями)
	K        uint     // Количество хэш-функций
	Salts    []uint64 // Соли для хэш-функций
}

// BloomFilter объект фильтра Блума с предустановленными параметрами
type BloomFilter struct {
	config *BloomFilterConfig
}

// newBloomFilter создает новый фильтр Блума с заданными параметрами
func newBloomFilter(size uint64, k uint) *BloomFilter {
	// Генерируем соли
	salts := make([]uint64, k)
	for i := uint(0); i < k; i++ {
		salts[i] = generateSalt(i)
	}
	
	config := &BloomFilterConfig{
		Size:  size,
		K:     k,
		Salts: salts,
	}
	
	return &BloomFilter{
		config: config,
	}
}

// NewBloomFilterWithConfig создает фильтр с готовой конфигурацией
func NewBloomFilterWithConfig(config *BloomFilterConfig) *BloomFilter {
	return &BloomFilter{
		config: config,
	}
}

// NewFilter создает оптимизированный фильтр на основе ожидаемых параметров
func NewFilter(expectedElements uint64, falsePositiveRate float64) *BloomFilter {
	size := optimalSize(expectedElements, falsePositiveRate)
	k := optimalHashFunctions(expectedElements, size)
	return newBloomFilter(size, k)
}

// GenerateMask создает битовую маску на основе массива ключей
func (bf *BloomFilter) GenerateMask(keys [][]byte) []byte {
	// Вычисляем размер маски в байтах
	byteSize := (bf.config.Size + 7) / 8
	mask := make([]byte, byteSize)
	
	for _, key := range keys {
		bf.addKeyToMask(key, mask)
	}
	
	return mask
}

// GenerateMaskFromStrings создает маску из строковых ключей
func (bf *BloomFilter) GenerateMaskFromStrings(keys []string) []byte {
	byteKeys := make([][]byte, len(keys))
	for i, key := range keys {
		byteKeys[i] = []byte(key)
	}
	return bf.GenerateMask(byteKeys)
}

// Test проверяет ключ на наличие в маске
func (bf *BloomFilter) Test(key []byte, mask []byte) bool {
	for i := uint(0); i < bf.config.K; i++ {
		position := bf.calculateHashPosition(key, i)
		if !bf.isBitSet(position, mask) {
			return false
		}
	}
	return true
}

// TestString проверяет строковый ключ
func (bf *BloomFilter) TestString(key string, mask []byte) bool {
	return bf.Test([]byte(key), mask)
}

// AddKeyToMask добавляет один ключ в существующую маску
func (bf *BloomFilter) AddKeyToMask(key []byte, mask []byte) {
	bf.addKeyToMask(key, mask)
}

// addKeyToMask внутренний метод добавления ключа в маску
func (bf *BloomFilter) addKeyToMask(key []byte, mask []byte) {
	for i := uint(0); i < bf.config.K; i++ {
		position := bf.calculateHashPosition(key, i)
		bf.setBit(position, mask)
	}
}

// calculateHashPosition вычисляет позицию в битовом массиве для ключа и хэш-функции
func (bf *BloomFilter) calculateHashPosition(key []byte, hashIndex uint) uint64 {
	h := fnv.New64a()
	
	// Добавляем соль
	saltBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(saltBytes, bf.config.Salts[hashIndex])
	h.Write(saltBytes)
	
	// Добавляем ключ
	h.Write(key)
	
	hashValue := h.Sum64()
	return hashValue % bf.config.Size
}

// setBit устанавливает бит в маске
func (bf *BloomFilter) setBit(position uint64, mask []byte) {
	byteIndex := position / 8
	bitIndex := position % 8
	mask[byteIndex] |= 1 << bitIndex
}

// isBitSet проверяет установлен ли бит в маске
func (bf *BloomFilter) isBitSet(position uint64, mask []byte) bool {
	byteIndex := position / 8
	bitIndex := position % 8
	return (mask[byteIndex] & (1 << bitIndex)) != 0
}

// GetConfig возвращает конфигурацию фильтра
func (bf *BloomFilter) GetConfig() *BloomFilterConfig {
	return bf.config
}

// GetMaskSize возвращает размер маски в байтах
func (bf *BloomFilter) GetMaskSize() int {
	return int((bf.config.Size + 7) / 8)
}

// Utility functions

func optimalSize(n uint64, p float64) uint64 {
	if p <= 0 || p >= 1 {
		p = 0.01
	}
	return uint64(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2))
}

func optimalHashFunctions(n, m uint64) uint {
	return uint(math.Ceil(float64(m) / float64(n) * math.Ln2))
}

func generateSalt(index uint) uint64 {
	// Используем разные стратегии генерации соли
	switch index % 4 {
	case 0:
		return uint64(index) * 0x9e3779b97f4a7c15 // золотое сечение
	case 1:
		return uint64(index) * 0xbf58476d1ce4e5b9 // другая константа
	case 2:
		return uint64(index) * 0x94d049bb133111eb // еще одна
	default:
		return uint64(index) * 0x5fe6ec49e2f6e415
	}
}
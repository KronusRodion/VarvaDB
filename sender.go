package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Структура для JSON данных
type RequestData struct {
	Key 	string 		`json:"key"`
	Value 	string      `json:"value"`
}

// Конфигурация
const (
	url           = "http://localhost:8080/api/v1/" // Замените на ваш URL
	totalRequests = 1000
	maxGoroutines = 300
)

func main() {
	start := time.Now()
	
	// Создаем канал для ограничения количества горутин
	semaphore := make(chan struct{}, maxGoroutines)
	
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	errorCount := 0
	
	fmt.Printf("Начинаем отправку %d запросов с %d горутинами...\n", totalRequests, maxGoroutines)
	
	for i := 1000; i < totalRequests+1000; i++ {
		wg.Add(1)
		semaphore <- struct{}{} // Занимаем слот
		
		go func(requestID int) {
			defer wg.Done()
			defer func() { <-semaphore }() // Освобождаем слот
			
			if err := sendRequest(requestID); err != nil {
				log.Printf("Ошибка в запросе %d: %v", requestID, err)
				mu.Lock()
				errorCount++
				mu.Unlock()
			} else {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}
	
	wg.Wait()
	close(semaphore)
	
	duration := time.Since(start)
	fmt.Printf("\n=== Результаты ===\n")
	fmt.Printf("Успешных запросов: %d\n", successCount)
	fmt.Printf("Ошибок: %d\n", errorCount)
	fmt.Printf("Общее время: %v\n", duration)
	fmt.Printf("Запросов в секунду: %.2f\n", float64(totalRequests)/duration.Seconds())
}

func sendRequest(requestID int) error {
	// Создаем данные для отправки
	key := strconv.Itoa(requestID)
	data := RequestData{
		Value: fmt.Sprintf("value_%d", requestID),
		Key: key,
	}
	
	// Сериализуем в JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("ошибка маршалинга JSON: %w", err)
	}
	
	// Создаем HTTP запрос
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("ошибка создания запроса: %w", err)
	}
	
	// Устанавливаем заголовки
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Go-Client/1.0")
	
	// Выполняем запрос
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer resp.Body.Close()
	
	// Проверяем статус код
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("неуспешный статус код: %d", resp.StatusCode)
	}
	
	return nil
}
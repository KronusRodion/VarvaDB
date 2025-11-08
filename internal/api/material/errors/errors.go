package errors


import (
	"encoding/json"
	"fmt"
	"net/http"
)

// ResponseError - структура ошибки в ответ на http запрос
type ResponseError struct {
	Message string `json:"message"`          
	Err   	string `json:"error"`           
	Code  	int    `json:"code,omitempty"`  
}

// Error реализует интерфейс error
func (e *ResponseError) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s, Error: %s", e.Code, e.Message, e.Err)
}

// Write записывает ошибку в ResponseWriter
func (e *ResponseError) Write(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Code)
	json.NewEncoder(w).Encode(e)
}

// ComposeError создает новый ResponseError
func ComposeError(code int, message string, err error) *ResponseError {
	responseErr := &ResponseError{
		Code:  code,
		Message: message,
		Err: err.Error(),
	}

	return responseErr
}
package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"varvaDB/internal/api/material/errors"
	"varvaDB/internal/api/material/usecase"

	"github.com/gorilla/mux"
)

type DataHandler struct {
	uc usecase.Data

}

var _ Data = &DataHandler{}

func NewData(uc usecase.Data) *DataHandler {
	return &DataHandler{uc:uc}
}

func (d *DataHandler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/{key}", d.handleGet).Methods(http.MethodGet)
	router.HandleFunc("/{key}", d.handleDelete).Methods(http.MethodDelete)
	router.HandleFunc("/", d.handlePost).Methods(http.MethodPost)
}

func (d *DataHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	
	w.Header().Set("Content-Type","application/json")

	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		errResponse := errors.ComposeError(http.StatusBadRequest, "empty key", fmt.Errorf("key not found"))
		errResponse.Write(w)
		return
	}

	log.Println("key: ",key)
	value, exist := d.uc.Get(key)
	if !exist {
		errResponse := errors.ComposeError(http.StatusNotFound, "Not Found", fmt.Errorf("value not found"))
		errResponse.Write(w)
		return
	}

	type response struct {
		Key		string	`json:"key"`
		Value 	[]byte	`json:"value"`
	}

	resp := response{Key: key, Value: value}

	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(resp)
	if err != nil {
		log.Println(err)
		errResponse := errors.ComposeError(http.StatusNotFound, "json encoder error", err)
		errResponse.Write(w)
	}
}

func (d *DataHandler) handlePost(w http.ResponseWriter, r *http.Request) {
	
	w.Header().Set("Content-Type","application/json")

	var request struct {
		Key   string `json:"key"`
        Value string `json:"value"`
    }
    
    err := json.NewDecoder(r.Body).Decode(&request)
    if err != nil {
        errResponse := errors.ComposeError(http.StatusBadRequest, "invalid JSON", err)
        errResponse.Write(w)
        return
    }

	if request.Key == "" || request.Value == "" {
		errResponse := errors.ComposeError(http.StatusBadRequest, "empty parametr", fmt.Errorf("key or value is empty"))
        errResponse.Write(w)
        return
	}

	key := []byte(request.Key)
	value := []byte(request.Value)

	d.uc.Put(key, value)

	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(request)
	if err != nil {
		log.Println(err)
		errResponse := errors.ComposeError(http.StatusNotFound, "json encoder error", err)
		errResponse.Write(w)
	}
}


func (d *DataHandler) handleDelete(w http.ResponseWriter, r *http.Request) {
	
	w.Header().Set("Content-Type","application/json")

	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		errResponse := errors.ComposeError(http.StatusBadRequest, "empty key", fmt.Errorf("key not found"))
		errResponse.Write(w)
		return
	}

	d.uc.Delete(key)

	w.WriteHeader(http.StatusOK)
}


package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"varvaDB/config"
	"varvaDB/internal/core/engine"
	"varvaDB/internal/data"

	"github.com/gorilla/mux"
)

func main() {

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Println("Ошибка загрузки конфигурации: ", err)
		return
	}

	lsm, err := engine.NewLSM(cfg)
	if err != nil {
		log.Println("Ошибка при создании lsm дерева: ", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	core, err := lsm.BuildCore(ctx)
	if err != nil {
		log.Println("Ошибка при сборке ядра: ", err)
	}
	log.Println("core построен")

	engine := engine.New(core)
	engine.Start(ctx)
	

	router := mux.NewRouter()
	r := router.PathPrefix("/api/v1").Subrouter()

	module := data.NewModule(core)
	module.Register(r)

	r.Use(loggingMiddleware)
	

	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		port := ":8080"
		log.Println("Сервер запущен на порту ", port)
		err := http.ListenAndServe(port, r)
		if err != nil {
			log.Println("Ошибка сервера: ", err)
			sigChan <- syscall.SIGTERM
		}
	}()


	sig := <-sigChan
	log.Printf("Пришел сигнал %s, останавливаем СУБД...", sig)

}


func loggingMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        
		now := time.Now()
        next.ServeHTTP(w, r)
		log.Printf("Received request: %s %s, time: %d microseconds", r.Method, r.URL.Path, time.Since(now).Microseconds())
    })
}

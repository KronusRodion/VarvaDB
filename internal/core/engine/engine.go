package engine

import (
	"context"
	"log"
	"time"
)

type Core interface {
	Start(ctx context.Context)
	Info()
	Put(key, value []byte) error
	Delete(key string)
	Get(key string) ([]byte, bool)
}

type Engine struct {
	core Core
}

func New(core Core) *Engine {
	return &Engine{core: core}
}

func (e *Engine) Start(ctx context.Context) {
	e.core.Start(ctx)
	log.Println("core запущен")
	ticker := time.NewTicker(time.Second * 10)

	go func() {
		for {
			select{
			case <- ctx.Done():
				log.Println("Engine остановлен по контексту")
				ticker.Stop()
				return
			case <- ticker.C:
				e.core.Info()
			}
		}
		}()
	log.Println("Engine запущен")
	
}

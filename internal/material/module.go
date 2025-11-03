package data

import (
	"varvaDB/internal/core/engine"
	"varvaDB/internal/data/handlers"
	"varvaDB/internal/data/usecase"

	"github.com/gorilla/mux"
)

type module struct {
	dataHandler handlers.Data
	dataUsecase usecase.Data
}

func NewModule(core engine.Core) *module {

	uc := usecase.New(core)
	handler := handlers.NewData(uc)

	return &module{
		dataUsecase: uc,
		dataHandler: handler,
	}
}

func (m *module) Register(r *mux.Router) {
	m.dataHandler.RegisterRoutes(r)
}

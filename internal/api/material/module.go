package data

import (
	"varvaDB/internal/core/engine"
	"varvaDB/internal/api/material/handlers"
	"varvaDB/internal/api/material/usecase"

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

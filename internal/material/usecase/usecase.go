package usecase

import "varvaDB/internal/core/engine"

type dataUsecase struct {
	core	engine.Core
}

var _ Data = &dataUsecase{}

func New(core engine.Core) *dataUsecase {
	return &dataUsecase{
		core: core,
	}
}

func (d *dataUsecase) Get(key string) ([]byte, bool) {
	return d.core.Get(key)
}

func (d *dataUsecase) Delete(key string) {
	d.core.Delete(key)
}

func (d *dataUsecase) Put(key, value []byte) {
	d.core.Put(key, value)
}
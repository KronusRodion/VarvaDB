package handlers

import "github.com/gorilla/mux"

type Data interface {
	RegisterRoutes(router *mux.Router)
}
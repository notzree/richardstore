package main

import "net/http"

type HTTPClientHandler struct {
	cli Client
	mux http.ServeMux
}

func NewHTTPClientHandler(cli Client) *HTTPClientHandler {
	return &HTTPClientHandler{
		cli: cli,
		mux: *http.NewServeMux(),
	}
}

func (s *HTTPClientHandler) Run() {
	s.mux.HandleFunc("/get", s.Get)
	s.mux.HandleFunc("/put", s.Put)
	s.mux.HandleFunc("/delete", s.Delete)
}

func (s *HTTPClientHandler) Get(w http.ResponseWriter, r *http.Request) {

}

func (s *HTTPClientHandler) Put(w http.ResponseWriter, r *http.Request) {

}

func (s *HTTPClientHandler) Delete(w http.ResponseWriter, r *http.Request) {

}

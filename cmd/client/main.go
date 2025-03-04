package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	hdfs "github.com/notzree/richardstore/hdfs"
	"github.com/notzree/richardstore/store"
)

type Server struct {
	client *hdfs.Client
}

type UploadResponse struct {
	Hash string `json:"hash"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func NewServer(nameNodeAddr string, storer *store.Store) (*Server, error) {
	client, err := hdfs.NewClient(nameNodeAddr, storer)
	if err != nil {
		return nil, err
	}
	return &Server{client: client}, nil
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse multipart form with 32MB max memory
	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		writeError(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		writeError(w, "Failed to get file from form", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "upload-*")
	if err != nil {
		writeError(w, "Failed to create temporary file", http.StatusInternalServerError)
		return
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Copy the uploaded file to the temporary file
	_, err = io.Copy(tempFile, file)
	if err != nil {
		writeError(w, "Failed to copy file", http.StatusInternalServerError)
		return
	}

	// Seek to the beginning of the file
	_, err = tempFile.Seek(0, 0)
	if err != nil {
		writeError(w, "Failed to seek file", http.StatusInternalServerError)
		return
	}

	// Write the file to HDFS
	hash, err := s.client.WriteFile(tempFile)
	if err != nil {
		writeError(w, fmt.Sprintf("Failed to write file: %v", err), http.StatusInternalServerError)
		return
	}

	response := UploadResponse{
		Hash: hash,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hash := r.URL.Query().Get("hash")
	if hash == "" {
		writeError(w, "Hash parameter is required", http.StatusBadRequest)
		return
	}

	reader, err := s.client.ReadFile(hash)
	if err != nil {
		writeError(w, fmt.Sprintf("Failed to read file: %v", err), http.StatusInternalServerError)
		return
	}
	defer (*reader).Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", hash))

	_, err = io.Copy(w, *reader)
	if err != nil {
		log.Printf("Error copying file to response: %v", err)
		// Note: Can't write error response here as we've already started writing the response
	}
}

func writeError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

func main() {
	nameNodeAddr := os.Getenv("NAMENODE_ADDRESS")
	if nameNodeAddr == "" {
		nameNodeAddr = ":3009"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = ":8080"
	}
	// Initialize your store
	storer := store.NewStore(store.StoreOpts{
		BlockSize: 5,
		Root:      "root",
	}) // Adjust based on your actual store initialization

	server, err := NewServer(nameNodeAddr, storer) // Adjust the namenode address
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	http.HandleFunc("/upload", server.handleUpload)
	http.HandleFunc("/download", server.handleDownload)

	log.Printf("Starting server on port %s", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

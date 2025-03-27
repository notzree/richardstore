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
	hash, err := s.client.WriteFile(tempFile, 0.7)
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

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// In a real implementation, you'd fetch this from your namenode
	info := map[string]interface{}{
		"service":  "Distributed File System",
		"status":   "running",
		"namenode": "s.client.GetNameNodeAddress()", //TODO: implement this later?
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func writeError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

func main() {
	// Get environment variables or use defaults
	nameNodeAddr := os.Getenv("NAMENODE_ADDRESS")
	if nameNodeAddr == "" {
		nameNodeAddr = ":3009"
	}
	log.Printf("Using NameNode address: %s", nameNodeAddr)

	port := os.Getenv("PORT")
	if port == "" {
		port = ":8080"
	}
	log.Printf("Starting client server on port %s", port)

	// Use data directory for client cache
	dataRoot := "/data/client"
	if _, err := os.Stat("/data"); os.IsNotExist(err) {
		// Fallback for local development
		dataRoot = "client-data"
		// Ensure the directory exists
		os.MkdirAll(dataRoot, 0755)
	}
	log.Printf("Using data root: %s", dataRoot)

	// Initialize store
	storer := store.NewStore(store.StoreOpts{
		BlockSize: 5,
		Root:      dataRoot,
	})

	// Create server
	server, err := NewServer(nameNodeAddr, storer)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Set up routes
	http.HandleFunc("/upload", server.handleUpload)
	http.HandleFunc("/download", server.handleDownload)
	http.HandleFunc("/info", server.handleInfo)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `<!DOCTYPE html>
<html>
<head>
  <title>Distributed File System</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
    h1 { color: #333; }
    form { margin: 20px 0; padding: 20px; border: 1px solid #ddd; border-radius: 5px; }
    input[type="submit"] { background: #4CAF50; color: white; padding: 10px 15px; border: none; cursor: pointer; }
  </style>
</head>
<body>
  <h1>Distributed File System</h1>
  <form action="/upload" method="post" enctype="multipart/form-data">
    <h2>Upload File</h2>
    <input type="file" name="file" required>
    <input type="submit" value="Upload">
  </form>
  <form action="/download" method="get">
    <h2>Download File</h2>
    <input type="text" name="hash" placeholder="File Hash" required>
    <input type="submit" value="Download">
  </form>
</body>
</html>`)
	})

	// Start server
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

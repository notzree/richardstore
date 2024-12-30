package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"time"
)

type DataPlane struct {
	http.Server
	DataSender
	Store
	port string
}

func NewDataPlane(port string) *DataPlane {
	return &DataPlane{
		port:       port,
		DataSender: *NewDataSender(),
		Store:      *NewStore(StoreOpts{blockSize: 5, root: port}),
	}
}

func (dp *DataPlane) Listen() {
	http.HandleFunc("/p2p/transfer", dp.AcceptTransfer)
	fmt.Printf("Server starting on port %s\n", dp.port)
	if err := http.ListenAndServe(dp.port, nil); err != nil {
		log.Fatal(err)
	}
}
func (dp *DataPlane) AcceptTransfer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Parse the multipart form data to get the file
	if err := r.ParseMultipartForm(32 << 20); err != nil { // 32MB max memory
		http.Error(w, "Error parsing multipart form", http.StatusBadRequest)
		return
	}

	uploadedFile, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Error retrieving file from request", http.StatusBadRequest)
		return
	}
	defer uploadedFile.Close()

	savedFileHash, err := dp.Store.Write(uploadedFile)
	if err != nil {
		log.Printf("failed to store uploaded file: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "success",
		"hash":   savedFileHash,
	})
}

func (dp *DataPlane) Teardown() error {
	// Create a context with timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// First shutdown the HTTP server gracefully
	if err := dp.Server.Shutdown(ctx); err != nil {
		return fmt.Errorf("error shutting down server: %w", err)
	}

	// Then clear the store
	if err := dp.Store.Clear(); err != nil {
		return fmt.Errorf("error clearing store: %w", err)
	}

	return nil
}

// wrappers for calling the http methods just to simplify life
type DataSender struct {
	client *http.Client
}

func NewDataSender() *DataSender {
	return &DataSender{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (ds *DataSender) Send(file io.ReadCloser, addr string) error {
	defer file.Close()

	pr, pw := io.Pipe()

	writer := multipart.NewWriter(pw)

	// Start goroutine to write file data
	go func() {
		defer pw.Close()
		defer writer.Close()

		part, err := writer.CreateFormFile("file", "upload")
		if err != nil {
			pw.CloseWithError(err)
			return
		}

		if _, err := io.Copy(part, file); err != nil {
			pw.CloseWithError(err)
			return
		}
	}()

	// Create request
	req, err := http.NewRequest(http.MethodPost, addr, pr)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set the content type with the boundary
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Send request
	resp, err := ds.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned non-200 status code: %d, body: %s",
			resp.StatusCode, string(body))
	}

	return nil
}

func writeJSON(w http.ResponseWriter, s int, v any) {
	w.WriteHeader(s)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("error writing json %v", v)
	}
}

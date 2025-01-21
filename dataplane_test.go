package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDataSender(t *testing.T) {
	var PORT string
	PORT = ":4001"
	tests := []struct {
		name     string
		data     string
		endpoint string
		wantErr  bool
	}{
		{
			name:     "basic transfer",
			data:     "test data",
			endpoint: "/p2p/transfer",
			wantErr:  false,
		},
		{
			name:     "empty data",
			data:     "",
			endpoint: "/p2p/transfer",
			wantErr:  false,
		},
		{
			name:     "large data",
			data:     strings.Repeat("large data ", 1000),
			endpoint: "/p2p/transfer",
			wantErr:  false,
		},
	}
	dp := NewDataPlane(PORT, NewStore(StoreOpts{5, PORT}))
	go dp.Listen()
	defer dp.Teardown()
	time.Sleep(500 * time.Millisecond)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test data
			buf := bytes.NewBuffer([]byte(tt.data))
			hashReader := bytes.NewBuffer([]byte(tt.data))
			hash := fmt.Sprintf("%x", sha256.Sum256(hashReader.Bytes()))
			// Create sender and attempt transfer
			ds := NewDataSender()
			err := ds.Send(io.NopCloser(buf),
				fmt.Sprintf("http://localhost%s%s", PORT, tt.endpoint))
			if (err != nil) != tt.wantErr {
				t.Errorf("DataSender.Send() error = %v, wantErr %v", err, tt.wantErr)
			}

			dataReader, err := dp.Store.Read(hash)
			if err != nil {
				t.Errorf("data read failed: %v", err)
			}
			readData, err := io.ReadAll(dataReader)
			assert.Equal(t, readData, []byte(tt.data))

		})
	}
}

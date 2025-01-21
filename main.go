package main

import (
	"strconv"
	"strings"
	"time"

	"github.com/notzree/richardstore/proto"
)

func main() {
	// dp := NewDataPlane(":4000")
	// go dp.Listen()
	// time.Sleep(500 * time.Millisecond)
	// ds := NewDataSender()
	// data := "momsbestpicture"
	// buf := bytes.NewBuffer([]byte(data))
	// err := ds.Send(io.NopCloser(buf), "http://localhost:4000/p2p/transfer")
	// if err != nil {
	// 	fmt.Println(err)
	// }
	MAXSIMCOMMANDS := 5
	NUMNODES := 5
	const (
		KB          = 1024
		MB          = 1024 * KB
		sizeInBytes = 50 * MB
	)
	nameNode := NewNameNode(0, ":3009", nil, 3*time.Second, MAXSIMCOMMANDS)
	dataNodes := make([]DataNode, NUMNODES+1)
	peerDataNodes := make([]PeerDataNode, NUMNODES+1)
	for i := 1; i < (NUMNODES + 1); i++ {
		address := ":300" + strconv.Itoa(i)
		storeOpts := StoreOpts{
			blockSize: 5,
			root:      address,
		}
		dataNodes[i] = *NewDataNode(uint64(i), address, NewStore(storeOpts), MAXSIMCOMMANDS, 50*MB)
		peerDataNodes[i] = *dataNodes[i].PeerRepresentation()
	}

	nameNode.AddDataNodes(peerDataNodes)
	go nameNode.Run()

	for i := 1; i < (NUMNODES + 1); i++ {
		dataNodes[i].NameNode = *nameNode.PeerRepresentation()

		// Create peer list excluding self
		peers := make([]PeerDataNode, 0)
		for j := 1; j < (NUMNODES + 1); j++ {
			if j != i { // Don't add self as peer
				peers = append(peers, peerDataNodes[j])
			}
		}
		dataNodes[i].AddDataNodes(peers)
		go dataNodes[i].Run()
	}

	testData := "momsbestpicture"
	hash, err := dataNodes[1].Storer.Write(strings.NewReader(testData))
	if err != nil {
		panic(err)
	}

	mockReplicationCmd := &proto.Command{
		Command: &proto.Command_Replicate{
			Replicate: &proto.ReplicateCommand{
				FileInfo: &proto.FileInfo{
					Hash:            hash,
					Size:            0,
					GenerationStamp: uint64(time.Now().UnixNano()),
				},
				TargetNodes: []uint64{2, 3},
			},
		},
	}
	time.Sleep(time.Second * 1)
	nameNode.Commands[1] = append(nameNode.Commands[1], mockReplicationCmd)
	dataNodes[1].SendHeartbeat()
	time.Sleep(time.Second * 100)

}

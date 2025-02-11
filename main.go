package main

import (
	"strconv"
	"time"
)

func main() {
	MAXSIMCOMMANDS := 5
	NUMNODES := 5
	const (
		KB          = 1024
		MB          = 1024 * KB
		sizeInBytes = 50 * MB
	)

	// TODO: Need to make Name node accept data nodes from heartbeat.
	// Datanodes however still need to know of each other from startup.

	nameNode := NewNameNode(0, ":3009", nil, 2*time.Second, 30*time.Second, MAXSIMCOMMANDS)
	dataNodes := make([]DataNode, NUMNODES+1)
	peerDataNodes := make([]PeerDataNode, NUMNODES+1)
	for i := 1; i < (NUMNODES + 1); i++ {
		address := ":300" + strconv.Itoa(i)
		storeOpts := StoreOpts{
			blockSize: 5,
			root:      address,
		}
		dataNodes[i] = *NewDataNode(uint64(i), address, NewStore(storeOpts), MAXSIMCOMMANDS, 5, 50*MB)
		peerDataNodes[i] = *dataNodes[i].PeerRepresentation()
	}

	// nameNode.AddDataNodes(peerDataNodes)
	go nameNode.Run()

	_, err := NewClient(":3009", NewStore(StoreOpts{
		blockSize: 5,
		root:      ":3009",
	}))
	if err != nil {
		panic(err)
	}
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

	// testData := "momsbestpicture"
	// hash, err := dataNodes[1].Storer.Write(strings.NewReader(testData))
	// if err != nil {
	// 	panic(err)
	// }

	// mockReplicationCmd := &proto.Command{
	// 	Command: &proto.Command_Replicate{
	// 		Replicate: &proto.ReplicateCommand{
	// 			FileInfo: &proto.FileInfo{
	// 				Hash:            hash,
	// 				Size:            0,
	// 				GenerationStamp: uint64(time.Now().UnixNano()),
	// 			},
	// 			TargetNodes: []uint64{2, 3},
	// 		},
	// 	},
	// }

	time.Sleep(time.Second * 10)
	// nameNode.Commands[1] = append(nameNode.Commands[1], mockReplicationCmd)
	// dataNodes[1].SendHeartbeat()
	// file, err := os.Open("test_file")
	// if err != nil {
	// 	panic(err)
	// }
	// client._write(file)
	// time.Sleep(time.Second * 100)

}

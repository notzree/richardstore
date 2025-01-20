package main

import (
	"strconv"
	"time"
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
	const (
		KB          = 1024
		MB          = 1024 * KB
		sizeInBytes = 50 * MB
	)
	nameNode := NewNameNode(0, ":3000", nil, 3*time.Second, 5)
	NUMNODES := 5
	MAXSIMCOMMANDS := 5
	dataNodes := make([]DataNode, NUMNODES+1)
	peerDataNodes := make([]PeerDataNode, NUMNODES+1)
	for i := 1; i < (NUMNODES + 1); i++ {
		address := ":300" + strconv.Itoa(i)
		storeOpts := StoreOpts{
			blockSize: 5,
			root:      address,
		}
		dataNodes[i] = *NewDataNode(uint64(i), address, NewStore(storeOpts), 50*MB)
		peerDataNodes[i] = *dataNodes[i].PeerRepresentation()
	}

	nameNode.AddDataNodes(peerDataNodes)

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

		dataNodes[i].InitializeClients()
		go func(idx int) { // Pass i as parameter
			dataNodes[idx].StartRPCServer()
			defer dataNodes[idx].CloseAllClients()
		}(i)
	}

	time.Sleep(time.Second * 500)

}

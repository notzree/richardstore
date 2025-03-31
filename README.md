# richardstore
richardstore is a distributed CAS file storage I built as a learning exercise.
richardstore is based off of the Hadoop distributed file system, and uses Go, k8s, and gRPC. Its also possible to run it without k8s if desired.
richardstore supports: read, write, delete, operations. 

## installation
requirements:
- Docker
- Go 1.23
- k3d
- kubectl
```bash
brew install k3d
brew install kubectl
```
Then you can read through deploy.sh, or just run:
```bash
chmod +x deploy.sh #to make it executible
bash deploy.sh
# You may need to run the following to enable port forwarding if not already enabled.
kubectl port-forward service/client 8080:80

```
This should create the required k3d cluster, build all the docker images, and run the cluster.
You should be able to go to http://localhost:8080/ and see this:

<img width="852" alt="image" src="https://github.com/user-attachments/assets/0d0f0acd-29fc-4932-961d-5448217f48b0" />


File Upload Demo:

https://github.com/user-attachments/assets/c7d479ca-6538-4401-b7d8-f7e4651a6608



## explanation
### CAS File storage
CAS (Content Addressable Storage) is a type of file system that stores data based on its content, rather than its location. This allows for efficient retrieval and deduplication of data.
This is all implemented in the store/storage.go file, where the Store class provides concurency safe methods for interacting with the storage system.
```go
// Example of writing a File
file, err := os.Open("my/file")
if err !=nil {
	log.Fatal(err)
}
defer file.Close()
hash, err := store.Write(file)
```
The hash of a file might look something like:
c0c61d6932fc8436ec0a8536ed0f191a0ef8b5e7eec9ac3c2657a7ce319388dc

And internally, this is broken down dependent on the blocksize. With a blocksize of 5, the actual file path may look something like:
root/c0c61/d6932/fc843/6ec0a/8536e/d0f19/1a0ef/8b5e7/eec9a/c3c26/57a7c/e319388dc/c0c61d6932fc8436ec0a8536ed0f191a0ef8b5e7eec9ac3c2657a7ce319388dc

Where the hash of the file itself is chunked to create the folders.

### Architecture
richardstore is similar to HDFS. It has a namenode, and a series of datanodes.
A namenode manages the file system metadata, things like location of files, which datanodes have which files, health of datanodes, etc.
The datanodes store the actual files (blocks in hdfs). All inter-system communication is done with gRPC, and the client exposes a REST HTTP endpoint for ease of use.
You can find the full hdfs architecture guide [here](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)

At a highlevel, this is what richardstore looks like:

<img width="986" alt="image" src="https://github.com/user-attachments/assets/8420186e-c474-43a6-ab3a-593fd243d354" />


### BlockReports
Blockreports are what the datanodes use to tell the namenode what files they have. The namenodes upon receiving this update their internal map of file -> datanodes for future read operations. A blockreport is basically just a list of all of the files that a datanode has. An incremental block report is essentially a compressed version of the blockreport which only tracks the changes that have happened on the file system (adding/deleting a file)


### Writing files (Replication chain)
To write a file, the client first asks the namenode where to write the file to.
The namenode will allocate space on the file system and return the addresses of the datanodes to write to. The client then directly writes to the first datanode of that list.
```proto
message FileStream {
    oneof type {
        StreamInfo stream_info = 1;
        bytes chunk = 2;
    }
}
message StreamInfo {
    FileInfo file_info =1;
    repeated DataNodeInfo data_nodes = 2;
}
```
You can see that in the StreamInfo, we can specify a list of datanodes that we want this file replicated to. The DataNode will write the file to its file system, and then initiate the replication change if needed. The client only gets a success when all nodes on the chain have confirmed that they have succesfully written the file.

Now, after writing the file directly to the datanode, they aren't immediately readable because the namenode is only aware of the intention to write the file, it doesnt know which datanodes have the file yet.
Eventually after some short interval, the datanodes will all send either a BlockReport or an IncrementalBlockReport to the namenode. This makes the namenode aware that the datanode is holding onto the file, and makes the file readable from this point.

### Reading files
The client provides the namenode with a hash, and this hash is used to lookup against the namenodes filemap to check which replicas have the file. The namenode then returns the list of replicas, which the client can directly read from for the file. The client currently reads sequentially down the list until a successful read is made, but an alternative possibility is that the client can try all connections concurrently and cancel the others upon getting a successful read.

### Deleting files
To delete files, the namenode will propogate a command to all Datanodes during the next heartbeat. The namenode also has to track a list of recently deleted files. This is because theres a possibility that datanodes will send the deleted file back during a blockreport (if they havent seen the delete command yet), and then the namenode will not be able to differentiate between:
Datanode has a new File, or datanode still has a deleted file.
### limitations:
Building all of hdfs would have taken me forever. For one, I didn't implement blocks, which are a core part of HDFS. I was already toying around with building a CAS file system and reusing that would make my life way easier.
Theres also some durability + auth features that I opted to not build.

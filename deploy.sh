#!/bin/bash
# This is a guide script - run each command step by step

# Step 1: Install k3d if not already installed
# brew install k3d

# Step 2: Create a k3d cluster
echo "Creating k3d cluster..."
k3d cluster create dfs-cluster \
  --servers 1 \
  --agents 3 \
  --port "8080:80@loadbalancer" \
  --port "3009:3009@loadbalancer"

# Step 3: Build Docker images
echo "Building Docker images..."
# Adjust paths to your Dockerfiles as needed
docker build -t namenode:latest -f cmd/namenode/Dockerfile .
docker build -t datanode:latest -f cmd/datanode/Dockerfile .
docker build -t client:latest -f cmd/client/Dockerfile .

# Step 4: Import images to k3d
echo "Importing images to k3d..."
k3d image import namenode:latest datanode:latest client:latest -c dfs-cluster

# Step 5: Create k8s directory and save manifest files
echo "Creating k8s directory..."
mkdir -p k8s

# Copy the content from the k8s-manifests-final artifact into k8s/dfs.yaml

# Step 6: Apply manifests
echo "Applying Kubernetes manifests..."
kubectl apply -f k8s/dfs.yaml

# Step 7: Check deployment status
echo "Checking pod status..."
kubectl get pods -w

# After pods are Running, check services
echo "Checking services..."
kubectl get services

# Helpful commands:

# Access logs
# kubectl logs -f namenode-xxxxxxxxx-xxxxx
# kubectl logs -f datanode-0

# Delete all resources
# kubectl delete -f k8s/dfs.yaml

# Delete cluster when done
# k3d cluster delete dfs-cluster

# Port forwarding (if needed)
# kubectl port-forward service/client 8080:80

# Scale the number of datanodes
# kubectl scale statefulset/datanode --replicas=5

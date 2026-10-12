#! /usr/bin/env python3
import os
import sys
import time
import argparse
import torch
import torch.nn as nn
import torch.optim as optim
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader, DistributedSampler, Subset
import torchvision
import torchvision.transforms as transforms
import torchvision.models as models

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--distributed', action='store_true')
    parser.add_argument('--epochs', type=int, default=1)
    parser.add_argument('--samples', type=int, default=1000) # Subset for faster benchmarking on CPU
    args = parser.parse_args()

    rank = 0
    world_size = 1
    
    if args.distributed:
        rank = int(os.environ["RANK"])
        world_size = int(os.environ["WORLD_SIZE"])
        dist.init_process_group(backend="gloo", rank=rank, world_size=world_size)
    
    # Standard ImageNet normalization for pre-trained models
    transform_train = transforms.Compose([
        transforms.Resize(64), # Scale up slightly for ResNet
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    
    if rank == 0:
        print(f"[Master] Loading CIFAR-10 dataset (may take a moment to verify/download)...", flush=True)
        
    # We assume data is already present from dispatch.sh to avoid network skew
    dataset = torchvision.datasets.CIFAR10(root="/tmp/cifar10_data", train=True, download=True, transform=transform_train)
    
    # Subset dataset for benchmarking to keep times reasonable on edge CPUs
    indices = list(range(args.samples))
    subset = Subset(dataset, indices)
    
    if args.distributed:
        sampler = DistributedSampler(subset, num_replicas=world_size, rank=rank)
        dataloader = DataLoader(subset, batch_size=32, sampler=sampler, num_workers=2)
    else:
        dataloader = DataLoader(subset, batch_size=32, shuffle=True, num_workers=2)
        
    if rank == 0:
        print("[Master] Downloading/Initializing pre-trained ResNet-18 weights (44MB)...", flush=True)
    
    # DOWNLOAD PRE-TRAINED MODEL
    # We use weights pre-trained on millions of ImageNet images
    model = models.resnet18(weights=models.ResNet18_Weights.DEFAULT)
    
    # Transfer Learning: Freeze the deep convolutional backbone so we don't compute gradients for them
    for param in model.parameters():
        param.requires_grad = False
        
    # Replace the final fully connected layer for 10 CIFAR classes (this layer WILL be trained)
    # The new layer automatically has requires_grad=True
    model.fc = nn.Linear(model.fc.in_features, 10)
    
    if args.distributed:
        model = DDP(model)
        
    criterion = nn.CrossEntropyLoss()
    # ONLY optimize the parameters that require gradients (the new FC layer) to save massive CPU time
    optimizer = optim.Adam(filter(lambda p: p.requires_grad, model.parameters()), lr=0.001)
    
    # --- START TIMING ---
    # Sync all nodes exactly before starting the timer
    if args.distributed:
        dist.barrier()
    
    start_time = time.time()
    
    for epoch in range(args.epochs):
        if args.distributed:
            sampler.set_epoch(epoch)
        
        model.train()
        for batch_idx, (inputs, targets) in enumerate(dataloader):
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()
            
            if rank == 0 and batch_idx % 10 == 0:
                print(f"[Master] Epoch {epoch} | Batch {batch_idx}/{len(dataloader)} | Loss: {loss.item():.4f}", flush=True)
            
    if args.distributed:
        dist.barrier()
        
    end_time = time.time()
    # --- END TIMING ---
    
    total_time = end_time - start_time
    
    if rank == 0:
        mode = "DISTRIBUTED" if args.distributed else "SINGLE"
        print(f"BENCHMARK_RESULT|{mode}|{total_time:.2f}")
        
    if args.distributed:
        dist.destroy_process_group()

if __name__ == "__main__":
    main()

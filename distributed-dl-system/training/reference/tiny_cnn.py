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
from torch.utils.data import DataLoader, DistributedSampler
import torchvision
import torchvision.transforms as transforms

class TinyYOLONet(nn.Module):
    def __init__(self, num_classes=10):
        super(TinyYOLONet, self).__init__()
        self.features = nn.Sequential(
            nn.Conv2d(3, 16, kernel_size=3, padding=1),
            nn.BatchNorm2d(16),
            nn.LeakyReLU(0.1),
            nn.MaxPool2d(2, 2),
            nn.Conv2d(16, 32, kernel_size=3, padding=1),
            nn.BatchNorm2d(32),
            nn.LeakyReLU(0.1),
            nn.MaxPool2d(2, 2),
            nn.Conv2d(32, 64, kernel_size=3, padding=1),
            nn.BatchNorm2d(64),
            nn.LeakyReLU(0.1),
            nn.MaxPool2d(2, 2),
        )
        self.classifier = nn.Sequential(
            nn.Flatten(),
            nn.Linear(64 * 4 * 4, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, num_classes),
        )

    def forward(self, x):
        x = self.features(x)
        x = self.classifier(x)
        return x

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--distributed', action='store_true')
    parser.add_argument('--epochs', type=int, default=1)
    args = parser.parse_args()

    rank = 0
    world_size = 1
    
    if args.distributed:
        rank = int(os.environ["RANK"])
        world_size = int(os.environ["WORLD_SIZE"])
        dist.init_process_group(backend="gloo", rank=rank, world_size=world_size)
    
    transform_train = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
    ])
    
    # We assume data is already present from dispatch.sh to avoid download skew
    dataset = torchvision.datasets.CIFAR10(root="/tmp/cifar10_data", train=True, download=True, transform=transform_train)
    
    if args.distributed:
        sampler = DistributedSampler(dataset, num_replicas=world_size, rank=rank)
        dataloader = DataLoader(dataset, batch_size=64, sampler=sampler, num_workers=2)
    else:
        dataloader = DataLoader(dataset, batch_size=64, shuffle=True, num_workers=2)
        
    model = TinyYOLONet(num_classes=10)
    if args.distributed:
        model = DDP(model)
        
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    
    # --- START TIMING ---
    # Sync all nodes exactly before starting the timer to avoid network lag skew
    if args.distributed:
        dist.barrier()
    
    start_time = time.time()
    
    for epoch in range(args.epochs):
        if args.distributed:
            sampler.set_epoch(epoch)
        
        model.train()
        for inputs, targets in dataloader:
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()
            
    if args.distributed:
        # Sync all nodes to ensure everyone finishes before stopping the clock
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

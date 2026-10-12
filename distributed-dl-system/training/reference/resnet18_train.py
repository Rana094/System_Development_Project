#! /usr/bin/env python3
import os
import argparse
import torch
import torch.nn as nn
import torch.optim as optim
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader, DistributedSampler
import torchvision
import torchvision.transforms as transforms
import torchvision.models as models

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--epochs', type=int, default=1)
    args = parser.parse_args()

    # DDP Initialization
    rank = int(os.environ["RANK"])
    world_size = int(os.environ["WORLD_SIZE"])
    dist.init_process_group(backend="gloo", rank=rank, world_size=world_size)
    
    transform_train = transforms.Compose([
        transforms.Resize(64),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    
    if rank == 0:
        print("[Master] Loading CIFAR-10 dataset...", flush=True)
        
    dataset = torchvision.datasets.CIFAR10(root="/tmp/cifar10_data", train=True, download=True, transform=transform_train)
    sampler = DistributedSampler(dataset, num_replicas=world_size, rank=rank)
    dataloader = DataLoader(dataset, batch_size=32, sampler=sampler, num_workers=2)
        
    if rank == 0:
        print("[Master] Initializing pre-trained ResNet-18...", flush=True)
    
    model = models.resnet18(weights=models.ResNet18_Weights.DEFAULT)
    
    for param in model.parameters():
        param.requires_grad = False
        
    model.fc = nn.Linear(model.fc.in_features, 10)
    model = DDP(model)
        
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(filter(lambda p: p.requires_grad, model.parameters()), lr=0.001)
    
    dist.barrier()
    
    for epoch in range(args.epochs):
        sampler.set_epoch(epoch)
        model.train()
        for batch_idx, (inputs, targets) in enumerate(dataloader):
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()
            
            if rank == 0 and batch_idx % 20 == 0:
                print(f"[Master] Epoch {epoch} | Batch {batch_idx}/{len(dataloader)} | Loss: {loss.item():.4f}", flush=True)
                
    dist.barrier()
    
    if rank == 0:
        print("[Master] Training complete. Saving combined weights...", flush=True)
        torch.save(model.module.state_dict(), "/tmp/pretrained_weights.pt")
        
    dist.destroy_process_group()

if __name__ == "__main__":
    main()

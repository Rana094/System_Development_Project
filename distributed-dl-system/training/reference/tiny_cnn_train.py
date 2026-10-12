#! /usr/bin/env python3
"""
Distributed Training Worker
Uses PyTorch's torch.distributed (Gloo backend) for true gradient-synced
distributed deep learning across Raspberry Pi compute nodes.

Architecture: TinyYOLO-style CNN (Conv2D -> BatchNorm -> ReLU -> MaxPool -> FC)
Dataset: CIFAR-10 (10 classes, 32x32 RGB images)
"""
import os
import sys
import json
import torch
import torch.nn as nn
import torch.optim as optim
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader, DistributedSampler
import torchvision
import torchvision.transforms as transforms


# ---------------------------------------------------------------------------
# Model Definition: A Tiny YOLO-inspired CNN
# ---------------------------------------------------------------------------
class TinyYOLONet(nn.Module):
    """
    A simplified YOLO-inspired convolutional neural network.
    Uses the same building blocks as real YOLO (Conv->BN->LeakyReLU)
    but scaled down massively to fit Raspberry Pi memory constraints.

    Architecture:
        Input (3x32x32)
        -> Conv2d(3, 16, 3)  -> BatchNorm -> LeakyReLU -> MaxPool2d
        -> Conv2d(16, 32, 3) -> BatchNorm -> LeakyReLU -> MaxPool2d
        -> Conv2d(32, 64, 3) -> BatchNorm -> LeakyReLU -> MaxPool2d
        -> Flatten -> FC(256, 128) -> ReLU -> Dropout
        -> FC(128, 10) -> Output
    """
    def __init__(self, num_classes=10):
        super(TinyYOLONet, self).__init__()

        # YOLO-style convolutional backbone
        self.features = nn.Sequential(
            # Block 1
            nn.Conv2d(3, 16, kernel_size=3, padding=1),
            nn.BatchNorm2d(16),
            nn.LeakyReLU(0.1),
            nn.MaxPool2d(2, 2),

            # Block 2
            nn.Conv2d(16, 32, kernel_size=3, padding=1),
            nn.BatchNorm2d(32),
            nn.LeakyReLU(0.1),
            nn.MaxPool2d(2, 2),

            # Block 3
            nn.Conv2d(32, 64, kernel_size=3, padding=1),
            nn.BatchNorm2d(64),
            nn.LeakyReLU(0.1),
            nn.MaxPool2d(2, 2),
        )

        # Classification head
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


# ---------------------------------------------------------------------------
# Distributed Training Setup
# ---------------------------------------------------------------------------
def setup_distributed():
    """
    Initialize PyTorch's distributed process group using the Gloo backend.
    Gloo is used because the Raspberry Pis have no GPUs (NCCL requires CUDA).

    Environment variables are set by the dispatch script:
      MASTER_ADDR  - IP/hostname of rank 0 node
      MASTER_PORT  - Port for the rendezvous server
      RANK         - Global rank of this process
      WORLD_SIZE   - Total number of processes
    """
    rank = int(os.environ["RANK"])
    world_size = int(os.environ["WORLD_SIZE"])

    dist.init_process_group(
        backend="gloo",
        rank=rank,
        world_size=world_size,
    )
    return rank, world_size


def cleanup():
    """Destroy the distributed process group and release network resources."""
    dist.destroy_process_group()


# ---------------------------------------------------------------------------
# Main Training Loop
# ---------------------------------------------------------------------------
def main():
    rank, world_size = setup_distributed()
    print(f"[Worker {rank}/{world_size}] PyTorch distributed initialized (Gloo backend)")

    # Data augmentation & normalization (standard CIFAR-10 preprocessing)
    transform_train = transforms.Compose([
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
    ])

    # Download CIFAR-10 dataset to a shared temp location
    dataset = torchvision.datasets.CIFAR10(
        root="/tmp/cifar10_data",
        train=True,
        download=True,
        transform=transform_train,
    )

    # DistributedSampler automatically shards the dataset across workers.
    # Rank 0 gets samples [0, 2, 4, ...], Rank 1 gets [1, 3, 5, ...], etc.
    sampler = DistributedSampler(dataset, num_replicas=world_size, rank=rank)

    dataloader = DataLoader(
        dataset,
        batch_size=64,
        sampler=sampler,
        num_workers=2,
        pin_memory=False,
    )

    # Initialize the model
    model = TinyYOLONet(num_classes=10)
    print(f"[Worker {rank}] Model parameters: {sum(p.numel() for p in model.parameters()):,}")

    # Wrap model with DistributedDataParallel (DDP).
    # DDP automatically synchronizes gradients across all nodes during backward().
    model = DDP(model)

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    # Training loop
    epochs = 5
    for epoch in range(epochs):
        sampler.set_epoch(epoch)  # Ensures different shuffling per epoch
        model.train()
        running_loss = 0.0
        correct = 0
        total = 0

        for batch_idx, (inputs, targets) in enumerate(dataloader):
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)

            # backward() triggers an all-reduce across all nodes via Gloo,
            # averaging the gradients before the optimizer step.
            loss.backward()
            optimizer.step()

            running_loss += loss.item()
            _, predicted = outputs.max(1)
            total += targets.size(0)
            correct += predicted.eq(targets).sum().item()

        acc = 100.0 * correct / total
        avg_loss = running_loss / len(dataloader)
        print(f"[Worker {rank}] Epoch {epoch+1}/{epochs} | Loss: {avg_loss:.4f} | Acc: {acc:.2f}%")

    # Save model weights (only rank 0 saves to avoid conflicts)
    if rank == 0:
        save_path = "/tmp/tinyyolo_distributed.pt"
        # .module accesses the underlying model inside DDP wrapper
        torch.save(model.module.state_dict(), save_path)
        print(f"[Worker {rank}] Model weights saved to {save_path}")

    cleanup()
    print(f"[Worker {rank}] Training complete. Distributed group destroyed.")


if __name__ == "__main__":
    main()

"""
Inference script - runs the trained TinyYOLO model on the PC (x86_64).
Loads the weight file produced by distributed training and classifies
CIFAR-10 test images.
"""
import sys
import torch
import torch.nn as nn
import torchvision
import torchvision.transforms as transforms
from torch.utils.data import DataLoader


class TinyYOLONet(nn.Module):
    """Same architecture as the training worker - must match exactly."""
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


CIFAR10_CLASSES = [
    "airplane", "automobile", "bird", "cat", "deer",
    "dog", "frog", "horse", "ship", "truck",
]


def main():
    weight_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/tinyyolo_distributed.pt"

    print("==================================================")
    print(" TINYYOLO INFERENCE (PC x86_64)                   ")
    print("==================================================")

    # Load model
    model = TinyYOLONet(num_classes=10)
    state_dict = torch.load(weight_path, map_location="cpu", weights_only=True)
    model.load_state_dict(state_dict)
    model.eval()
    print(f"[Inference] Loaded weights from {weight_path}")
    print(f"[Inference] Model parameters: {sum(p.numel() for p in model.parameters()):,}")

    # Load CIFAR-10 test set
    transform_test = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
    ])
    testset = torchvision.datasets.CIFAR10(
        root="/tmp/cifar10_data",
        train=False,
        download=True,
        transform=transform_test,
    )
    testloader = DataLoader(testset, batch_size=256, shuffle=False, num_workers=2)

    # Evaluate
    correct = 0
    total = 0
    class_correct = [0] * 10
    class_total = [0] * 10

    with torch.no_grad():
        for inputs, targets in testloader:
            outputs = model(inputs)
            _, predicted = outputs.max(1)
            total += targets.size(0)
            correct += predicted.eq(targets).sum().item()

            for i in range(targets.size(0)):
                label = targets[i].item()
                class_total[label] += 1
                if predicted[i] == label:
                    class_correct[label] += 1

    overall_acc = 100.0 * correct / total

    print(f"\n[Inference] Test set: {total} images")
    print(f"[Inference] Overall Accuracy: {overall_acc:.2f}%\n")
    print("Per-Class Accuracy:")
    print("-" * 35)
    for i in range(10):
        acc = 100.0 * class_correct[i] / class_total[i] if class_total[i] > 0 else 0
        print(f"  {CIFAR10_CLASSES[i]:>12s}: {acc:.1f}%")
    print("-" * 35)
    print("==================================================")


if __name__ == "__main__":
    main()

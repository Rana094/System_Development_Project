#! /usr/bin/env python3
import torch
import torch.nn as nn
import torchvision
import torchvision.transforms as transforms
import torchvision.models as models

def main():
    print("=========================================")
    print("  LOCAL INFERENCE: PRE-TRAINED RESNET-18 ")
    print("=========================================")
    
    transform_test = transforms.Compose([
        transforms.Resize(64),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    
    print("Loading CIFAR-10 Test Dataset...")
    testset = torchvision.datasets.CIFAR10(root='./data', train=False, download=True, transform=transform_test)
    testloader = torch.utils.data.DataLoader(testset, batch_size=100, shuffle=False, num_workers=2)
    
    print("Initializing ResNet-18 architecture...")
    model = models.resnet18(weights=None)
    model.fc = nn.Linear(model.fc.in_features, 10)
    
    print("Loading distributed trained weights (pretrained_weights.pt)...")
    try:
        model.load_state_dict(torch.load("pretrained_weights.pt", map_location=torch.device('cpu')))
    except Exception as e:
        print(f"Error loading weights: {e}")
        return
        
    model.eval()
    
    correct = 0
    total = 0
    print("Evaluating against test set...")
    
    with torch.no_grad():
        for inputs, targets in testloader:
            outputs = model(inputs)
            _, predicted = outputs.max(1)
            total += targets.size(0)
            correct += predicted.eq(targets).sum().item()
            
    acc = 100. * correct / total
    print(f"\nFinal Test Accuracy: {acc:.2f}%")
    print("=========================================")

if __name__ == "__main__":
    main()

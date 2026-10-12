#! /usr/bin/env python3
import os
import random

def generate_data(num_samples, rank):
    # True model: y = 4.5 * x1 - 2.0 * x2 + 1.5
    # We add random noise. Different ranks get different subsets/noise.
    random.seed(42 + rank)
    X = []
    y = []
    for _ in range(num_samples):
        x1 = random.uniform(-10, 10)
        x2 = random.uniform(-10, 10)
        noise = random.gauss(0, 1.0)
        target = 4.5 * x1 - 2.0 * x2 + 1.5 + noise
        X.append([x1, x2])
        y.append(target)
    return X, y

def train_sgd(X, y, epochs, lr):
    # Initialize weights and bias to 0
    w1, w2, bias = 0.0, 0.0, 0.0
    n = len(y)
    
    for epoch in range(epochs):
        for i in range(n):
            # Forward pass
            pred = w1 * X[i][0] + w2 * X[i][1] + bias
            error = pred - y[i]
            
            # Backpropagation (Gradient computation & update)
            w1 -= lr * error * X[i][0]
            w2 -= lr * error * X[i][1]
            bias -= lr * error
            
    return w1, w2, bias

if __name__ == "__main__":
    rank = int(os.environ.get("SLURM_PROCID", 0))
    size = int(os.environ.get("SLURM_NTASKS", 1))
    
    print(f"[Worker {rank}] Generating local dataset...")
    X_train, y_train = generate_data(5000, rank)
    
    print(f"[Worker {rank}] Training local model on 5000 samples...")
    # Train using simple Stochastic Gradient Descent
    w1, w2, bias = train_sgd(X_train, y_train, epochs=20, lr=0.001)
    
    # Output format that the master can easily parse
    print(f"--- MODEL_EXPORT RANK_{rank} ---")
    print(f"{w1},{w2},{bias}")

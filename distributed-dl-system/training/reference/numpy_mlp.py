#! /usr/bin/env python3
import os
import numpy as np
import argparse
import time

def generate_non_linear_data(samples, rank):
    np.random.seed(42 + rank)
    X = np.random.uniform(-1, 1, (samples, 2))
    y = (X[:, 0] * X[:, 1] > 0).astype(int).reshape(-1, 1)
    return X, y

def sigmoid(z): return 1.0 / (1.0 + np.exp(-z))
def sigmoid_deriv(a): return a * (1 - a)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--samples', type=int, required=True)
    args = parser.parse_args()

    rank = int(os.environ.get("SLURM_PROCID", 0))
    
    # Start the clock to profile exactly the data generation + training compute time
    start = time.time()
    
    X, y = generate_non_linear_data(args.samples, rank)
    np.random.seed(42)
    W1, b1 = np.random.randn(2, 4), np.zeros((1, 4))
    W2, b2 = np.random.randn(4, 1), np.zeros((1, 1))
    lr, epochs = 0.5, 500

    for epoch in range(epochs):
        # Forward pass (Vectorized)
        A1 = sigmoid(np.dot(X, W1) + b1)
        A2 = sigmoid(np.dot(A1, W2) + b2)
        
        # Backpropagation
        error = A2 - y
        dZ2 = error * sigmoid_deriv(A2)
        dW2 = np.dot(A1.T, dZ2) / args.samples
        db2 = np.sum(dZ2, axis=0, keepdims=True) / args.samples
        
        dA1 = np.dot(dZ2, W2.T)
        dZ1 = dA1 * sigmoid_deriv(A1)
        dW1 = np.dot(X.T, dZ1) / args.samples
        db1 = np.sum(dZ1, axis=0, keepdims=True) / args.samples
        
        # Update weights
        W2 -= lr * dW2
        b2 -= lr * db2
        W1 -= lr * dW1
        b1 -= lr * db1
        
    end = time.time()
    
    if rank == 0:
        print(f"TIME|{end - start:.3f}")

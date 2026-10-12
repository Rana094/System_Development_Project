#! /usr/bin/env python3
import os
import numpy as np

def generate_non_linear_data(samples, rank):
    np.random.seed(42 + rank)
    # Generate XOR-like data (non-linear, impossible for Logistic Regression)
    X = np.random.uniform(-1, 1, (samples, 2))
    # Label is 1 if x1*x2 > 0 else 0
    y = (X[:, 0] * X[:, 1] > 0).astype(int).reshape(-1, 1)
    return X, y

def sigmoid(z):
    return 1.0 / (1.0 + np.exp(-z))

def sigmoid_deriv(a):
    return a * (1 - a)

if __name__ == "__main__":
    rank = int(os.environ.get("SLURM_PROCID", 0))
    samples = 100000
    X, y = generate_non_linear_data(samples, rank)
    
    # 2-Layer MLP: Input(2) -> Hidden(4) -> Output(1)
    np.random.seed(42) # Initialize same starting weights for all nodes before SGD
    W1 = np.random.randn(2, 4)
    b1 = np.zeros((1, 4))
    W2 = np.random.randn(4, 1)
    b2 = np.zeros((1, 1))
    
    lr = 0.5
    epochs = 500
    
    print(f"[Worker {rank}] Training 2-Layer Neural Network (XOR dataset) on {samples} samples...")
    
    for epoch in range(epochs):
        # Forward pass (Vectorized for all samples simultaneously)
        Z1 = np.dot(X, W1) + b1
        A1 = sigmoid(Z1)
        Z2 = np.dot(A1, W2) + b2
        A2 = sigmoid(Z2)
        
        # Backpropagation
        error = A2 - y
        dZ2 = error * sigmoid_deriv(A2)
        dW2 = np.dot(A1.T, dZ2) / samples
        db2 = np.sum(dZ2, axis=0, keepdims=True) / samples
        
        dA1 = np.dot(dZ2, W2.T)
        dZ1 = dA1 * sigmoid_deriv(A1)
        dW1 = np.dot(X.T, dZ1) / samples
        db1 = np.sum(dZ1, axis=0, keepdims=True) / samples
        
        # Update weights
        W2 -= lr * dW2
        b2 -= lr * db2
        W1 -= lr * dW1
        b1 -= lr * db1
        
    print(f"--- MODEL_EXPORT RANK_{rank} ---")
    # Flatten everything to send back as strings
    w1_str = ",".join(map(str, W1.flatten()))
    b1_str = ",".join(map(str, b1.flatten()))
    w2_str = ",".join(map(str, W2.flatten()))
    b2_str = ",".join(map(str, b2.flatten()))
    
    print(f"{w1_str}|{b1_str}|{w2_str}|{b2_str}")

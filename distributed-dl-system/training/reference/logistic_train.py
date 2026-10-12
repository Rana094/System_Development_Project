#! /usr/bin/env python3
import os
import random
import math

def sigmoid(z):
    if z < -20: return 0.0
    if z > 20: return 1.0
    return 1.0 / (1.0 + math.exp(-z))

def dot(v1, v2):
    return sum(x*y for x, y in zip(v1, v2))

def generate_classification_data(samples, rank):
    """
    Generates synthetic binary classification data.
    True underlying weights: [1.5, -2.0, 0.5, 3.0, -1.0], Bias: -0.5
    """
    random.seed(42 + rank * 100) # Ensure diverse datasets across nodes
    X = []
    y = []
    true_w = [1.5, -2.0, 0.5, 3.0, -1.0]
    true_b = -0.5
    
    for _ in range(samples):
        features = [random.uniform(-5, 5) for _ in range(5)]
        # True logit
        z = dot(features, true_w) + true_b
        # Add normal noise to simulate real-world data complexity
        z += random.gauss(0, 1.5)
        # Convert to probability and then to binary label
        prob = sigmoid(z)
        label = 1 if prob >= 0.5 else 0
        X.append(features)
        y.append(label)
    return X, y

if __name__ == "__main__":
    rank = int(os.environ.get("SLURM_PROCID", 0))
    samples = 15000
    
    print(f"[Worker {rank}] Bootstrapping local dataset ({samples} rows, 5 features)...")
    X, y = generate_classification_data(samples, rank)
    
    # Initialize weights for Logistic Regression
    w = [0.0] * 5
    b = 0.0
    lr = 0.005 # Learning rate
    epochs = 40
    
    print(f"[Worker {rank}] Commencing Stochastic Gradient Descent (Epochs: {epochs})...")
    for epoch in range(epochs):
        for i in range(len(y)):
            # Forward pass
            pred = sigmoid(dot(w, X[i]) + b)
            error = pred - y[i] # Derivative of Log Loss with respect to z
            
            # Backpropagation / Weight Update
            for j in range(5):
                w[j] -= lr * error * X[i][j]
            b -= lr * error
            
    print(f"--- MODEL_EXPORT RANK_{rank} ---")
    weights_str = ",".join(map(str, w))
    print(f"{weights_str},{b}")

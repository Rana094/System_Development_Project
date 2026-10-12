#! /usr/bin/env python3
import os
import random
import math
import argparse
import time

def sigmoid(z):
    if z < -20: return 0.0
    if z > 20: return 1.0
    return 1.0 / (1.0 + math.exp(-z))

def dot(v1, v2):
    return sum(x*y for x, y in zip(v1, v2))

def generate_classification_data(samples, rank):
    random.seed(42 + rank * 100)
    X = []
    y = []
    true_w = [1.5, -2.0, 0.5, 3.0, -1.0]
    true_b = -0.5
    for _ in range(samples):
        features = [random.uniform(-5, 5) for _ in range(5)]
        z = dot(features, true_w) + true_b + random.gauss(0, 1.5)
        y.append(1 if sigmoid(z) >= 0.5 else 0)
        X.append(features)
    return X, y

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--samples', type=int, required=True)
    args = parser.parse_args()

    rank = int(os.environ.get("SLURM_PROCID", 0))
    
    # Start the clock to profile exactly the data generation + training compute time
    start = time.time()
    
    X, y = generate_classification_data(args.samples, rank)
    w, b = [0.0]*5, 0.0
    lr, epochs = 0.005, 40

    for epoch in range(epochs):
        for i in range(len(y)):
            pred = sigmoid(dot(w, X[i]) + b)
            error = pred - y[i]
            for j in range(5):
                w[j] -= lr * error * X[i][j]
            b -= lr * error

    end = time.time()
    if rank == 0:
        print(f"TIME|{end - start:.3f}")

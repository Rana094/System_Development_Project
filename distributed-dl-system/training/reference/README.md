# Reference workload explanations

These scripts are unchanged snapshots of the comparison project. Their
original locations and SHA-256 fingerprints are in `provenance.json`. Explanations
live here so that comments or formatting do not change those fingerprints.

| Script | What its main blocks do |
|---|---|
| `equation.py` | Generate synthetic inputs and targets; update three linear-model coefficients with gradient descent; export coefficients for averaging after all ranks finish. Each rank trains on 5,000 examples. |
| `logistic.py` | Generate a binary classification task; compute sigmoid predictions and SGD updates for 40 epochs; print the rank-zero compute timer. Two ranks train independent models on separate shards. |
| `numpy_mlp.py` | Build a 2→4→1 sigmoid network; run forward propagation and manually derived backpropagation for 500 epochs; print the rank-zero timer. The benchmark does not aggregate the independent rank models. |
| `resnet18.py` | Load CIFAR-10 and pretrained ResNet-18; freeze backbone parameters; replace the classifier with ten outputs; train it using cross-entropy and Adam. Distributed mode partitions the dataset and synchronizes gradients with PyTorch Gloo DDP. |
| `tiny_cnn.py` | Build three convolution blocks and a classifier; train all parameters on CIFAR-10 using cross-entropy and Adam; optionally partition data and synchronize gradients with Gloo DDP. Despite the original class name, this is an image classifier, not a bounding-box detector. |

The image scripts time the training loop, including their final DDP barrier,
after loading data and constructing the model. The adapter separately measures
the whole script; the comparison runner also measures queue-to-completion wall
time. These measurements answer different questions.

These reference scripts do not save recoverable checkpoints. Use
`training/pytorch_job.py` and `tools/run_reboot_demo.py` to demonstrate the
system's tested checkpoint recovery. One epoch of the image benchmarks measures
execution speed; it does not establish model convergence or held-out accuracy.

## Full workers

The `*_train.py` copies preserve the separate original `worker.py` programs:
logistic regression exports six parameters, and the NumPy MLP exports seventeen.
`resnet18_train.py` trains on the entire CIFAR-10 training set and exports a
state dictionary. `tiny_cnn_train.py` uses augmentation and five training epochs,
then exports its state dictionary. Both image workers initialize DDP even with
one rank. `*_inference.py` copies evaluate these weights on the CIFAR-10 test set.
The adapter supplies the original path/environment conventions and collects
results; none of these copied scripts are edited.

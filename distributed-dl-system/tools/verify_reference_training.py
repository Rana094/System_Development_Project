"""Verify completed reference jobs, exported models and independent test metrics."""

import argparse
import hashlib
import json
from pathlib import Path
import runpy

import numpy as np

ROOT = Path(__file__).resolve().parents[1]


def verify(directory):
    """Separate successful execution from model quality; retain both as evidence."""
    rows = {
        row["model"]: row
        for row in json.loads((directory / "results.json").read_text())
    }
    expected = {
        "equation",
        "logistic_train",
        "numpy_mlp_train",
        "resnet18_train",
        "tiny_cnn_train",
    }
    assert set(rows) == expected, "The full training matrix is incomplete"
    records = [json.loads(p.read_text()) for p in directory.glob("compare-*.json")]
    assert len(records) == 10, "Expected both ranks of all five training workflows"
    assert {(r["result"]["model"], r["result"]["rank"]) for r in records} == {
        (model, rank) for model in expected for rank in (0, 1)
    }, "A workload rank is missing or duplicated"
    assert sum(bool(r["result"].get("artifact")) for r in records) == 2
    for record in records:
        assert record["job"]["status"] == "COMPLETED"
        assert record["result"]["completed"]
        assert record["result"]["world_size"] == 2
        if record["result"].get("artifact"):
            result = record["result"]
            path = Path(result["controller_artifact"])
            assert (
                hashlib.sha256(path.read_bytes()).hexdigest()
                == result["artifact"]["sha256"]
            )
            assert result["artifact"]["finite"]
            assert result["test_accuracy_percent"] is not None

    provenance = json.loads((ROOT / "training/reference/provenance.json").read_text())
    for name, info in provenance.items():
        for path in (Path(info["source"]), ROOT / "training/reference" / f"{name}.py"):
            assert hashlib.sha256(path.read_bytes()).hexdigest() == info["sha256"], path

    # Use a previously unused rank seed to generate independent synthetic data.
    logistic = runpy.run_path(str(ROOT / "training/reference/logistic_train.py"))
    x, y = logistic["generate_classification_data"](10000, 999)
    w = rows["logistic_train"]["averaged_weights"]
    assert len(w) == 6 and np.isfinite(w).all()
    predicted = [logistic["dot"](w[:5], value) + w[5] >= 0 for value in x]
    logistic_accuracy = 100 * np.mean(np.asarray(predicted) == y)

    mlp = runpy.run_path(str(ROOT / "training/reference/numpy_mlp_train.py"))
    x, y = mlp["generate_non_linear_data"](10000, 999)
    w = np.asarray(rows["numpy_mlp_train"]["averaged_weights"])
    assert len(w) == 17 and np.isfinite(w).all()
    sigmoid = mlp["sigmoid"]
    prediction = sigmoid(
        sigmoid(x @ w[:8].reshape(2, 4) + w[8:12]) @ w[12:16].reshape(4, 1) + w[16]
    )
    mlp_accuracy = 100 * np.mean((prediction >= 0.5) == y)
    equation = np.asarray(rows["equation"]["averaged_weights"])
    assert len(equation) == 3 and np.isfinite(equation).all()
    result = {
        "execution_passed": True,
        "worker_jobs_completed": len(records),
        "reference_hashes_match": True,
        "equation_averaged_weights": equation.tolist(),
        "equation_target_weights": [4.5, -2.0, 1.5],
        "logistic_held_out_accuracy_percent": float(logistic_accuracy),
        "numpy_mlp_held_out_accuracy_percent": float(mlp_accuracy),
        "synthetic_test_samples": 10000,
        "synthetic_test_rank_seed": 999,
        "resnet18_test_accuracy_percent": rows["resnet18_train"][
            "test_accuracy_percent"
        ],
        "tiny_cnn_test_accuracy_percent": rows["tiny_cnn_train"][
            "test_accuracy_percent"
        ],
        "image_test_samples": 10000,
    }
    (directory / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
    report = [
        "# Private full-training verification",
        "",
        "All five original training workflows completed through the custom scheduler on pi02/pi03.",
        "The ten worker jobs completed, reference hashes match their originals, and both image models",
        "were copied to this directory with verified hashes and evaluated by the original inference scripts.",
        "",
        "| Workload | Original training configuration | Test accuracy | Total controller wall |",
        "|---|---|---:|---:|",
    ]
    settings = {
        "equation": ("5,000 samples per rank, 20 epochs", None),
        "logistic_train": ("15,000 samples per rank, 40 epochs", logistic_accuracy),
        "numpy_mlp_train": ("100,000 samples per rank, 500 epochs", mlp_accuracy),
        "resnet18_train": (
            "50,000 CIFAR-10 images total, 1 epoch",
            result["resnet18_test_accuracy_percent"],
        ),
        "tiny_cnn_train": (
            "50,000 CIFAR-10 images total, 5 epochs with augmentation",
            result["tiny_cnn_test_accuracy_percent"],
        ),
    }
    for name, (config, accuracy) in settings.items():
        value = "not applicable" if accuracy is None else f"{accuracy:.2f}%"
        report.append(
            f"| {name} | {config} | {value} | {rows[name]['controller_wall_seconds']:.2f}s |"
        )
    report.extend(
        [
            "",
            "## Interpretation",
            "",
            "Image accuracy uses the CIFAR-10 test set. Logistic/MLP accuracy uses 10,000 independent",
            "synthetic examples generated with rank seed 999 after averaging exported rank parameters.",
            "Execution success does not imply good convergence. In particular, the original NumPy MLP's",
            f"averaged model scored {mlp_accuracy:.2f}%; its learning code was preserved, not tuned to improve this result.",
            "",
            "Total wall time includes scheduling, training, image inference and worker-side validation.",
            "Controller artifact copying happens afterward. These times are not training-only benchmark times.",
            "The custom controller replaces Slurm launch scripts. Parameter averaging follows the original",
            "master scripts; their Slurm subprocess launch commands were not executed. The friend's original",
            "folder was not edited. These reference workers have no checkpoint recovery or automatic retry.",
            "",
            "Per-job JSON files contain original output, worker placement, attempts and artifact hashes.",
            "The .pt files are the freshly trained image-model state dictionaries. See verification.json for metrics.",
        ]
    )
    (directory / "verification_notes.md").write_text("\n".join(report) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    verify(parser.parse_args().directory.resolve())

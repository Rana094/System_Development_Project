"""Compare completed regression submissions, including exact learned tensor state."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "controller"))
from job_store import get_job


def equal(left, right):
    import torch
    if isinstance(left, torch.Tensor):
        return isinstance(right, torch.Tensor) and torch.equal(left, right)
    if isinstance(left, dict):
        return isinstance(right, dict) and left.keys() == right.keys() and all(
            equal(left[key], right[key]) for key in left
        )
    if isinstance(left, (list, tuple)):
        return isinstance(right, type(left)) and len(left) == len(right) and all(
            equal(a, b) for a, b in zip(left, right)
        )
    return left == right


def load_result(job_id):
    import torch
    job = get_job(job_id)
    if not job or job["job_type"] != "custom":
        raise ValueError(f"Unknown submitted job: {job_id}")
    if job["status"] != "COMPLETED":
        raise ValueError(f"{job_id} is {job['status']}; wait until it is COMPLETED")
    results = Path(job["storage_path"]) / "results"
    return (
        torch.load(results / "model.pt", map_location="cpu", weights_only=True),
        json.loads((results / "summary.json").read_text()),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline_job_id")
    parser.add_argument("recovered_job_id")
    args = parser.parse_args()
    try:
        baseline_model, baseline = load_result(args.baseline_job_id)
        recovered_model, recovered = load_result(args.recovered_job_id)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        parser.exit(1, str(error) + "\n")

    models_match = equal(baseline_model, recovered_model)
    mse_matches = baseline["final_mse"] == recovered["final_mse"]
    predictions_match = baseline["predictions"] == recovered["predictions"]
    print(json.dumps({
        "baseline": {"job_id": args.baseline_job_id, "steps": baseline["training_steps"],
                     "resumed_from": baseline["resumed_from"], "final_mse": baseline["final_mse"],
                     "predictions": baseline["predictions"]},
        "recovered": {"job_id": args.recovered_job_id, "steps": recovered["training_steps"],
                      "resumed_from": recovered["resumed_from"], "final_mse": recovered["final_mse"],
                      "predictions": recovered["predictions"]},
        "exact_model_optimizer_scheduler_match": models_match,
        "same_regression_mse": mse_matches,
        "same_predictions": predictions_match,
        "pass": models_match and mse_matches and predictions_match,
    }, indent=2))
    if not models_match or not mse_matches or not predictions_match:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

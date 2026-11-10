"""Plan disposable local failure evaluation; --run never touches live workers."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluation import faults, runner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true')
    parser.add_argument('--experiment-id', default=None)
    parser.add_argument('--output-root', type=Path, default=runner.ROOT / 'reports')
    parser.add_argument('--cases', nargs='+', choices=tuple(faults.CASES), default=list(faults.CASES))
    parser.add_argument('--timeout-seconds', type=int, default=120)
    args = parser.parse_args()
    settings = faults.Settings(args.experiment_id or runner.new_id(), tuple(args.cases), args.timeout_seconds)
    try:
        if not args.run:
            print(json.dumps(faults.plan(settings), indent=2, allow_nan=False))
            return 0
        folder, result = faults.run(settings, args.output_root.absolute())
        print(json.dumps(dict(output=str(folder), **result), indent=2))
        return int(result['status'] != 'LOCAL_PASS_LIVE_DEFERRED')
    except (ValueError, OSError) as error:
        parser.exit(2, f'{type(error).__name__}: {error}\n')


if __name__ == '__main__':
    raise SystemExit(main())

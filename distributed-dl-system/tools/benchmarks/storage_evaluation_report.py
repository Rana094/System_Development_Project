"""Validate collected records; --write creates a NEW offline report, never jobs."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluation import reporting, runner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inputs', nargs='+', required=True, type=Path)
    parser.add_argument('--report-id', default=None)
    parser.add_argument('--output-root', type=Path, default=runner.ROOT / 'reports')
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--charts', action='store_true', help='Requires optional controller-side matplotlib; never downloads it')
    args = parser.parse_args()
    if args.charts and not args.write: parser.error('--charts requires --write')
    try:
        data = reporting.collect([p.absolute() for p in args.inputs])
        if not args.write:
            print(json.dumps(dict(status='VALIDATED_READONLY', experiments=len(data), records=sum(len(d['records']) for d in data),
                scopes=sorted({d['metadata']['scope'] for d in data}), training_or_faults_started=False), indent=2))
            return 0
        target = reporting.write(data, args.output_root.absolute(), args.report_id or runner.new_id(), charts=args.charts)
        print(json.dumps(dict(status='REPORT_WRITTEN', output=str(target), physical_measurements_added=False), indent=2))
        return 0
    except (ValueError, OSError, ImportError) as error:
        parser.exit(2, f'{type(error).__name__}: {error}\n')


if __name__ == '__main__':
    raise SystemExit(main())

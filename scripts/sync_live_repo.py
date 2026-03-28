#!/usr/bin/env python3
import argparse
import json
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def run(cmd):
    return subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=True)


def existing_paths(paths):
    out = []
    for raw in paths:
        p = (REPO_ROOT / raw).resolve()
        try:
            rel = p.relative_to(REPO_ROOT)
        except ValueError:
            continue
        if p.exists():
            out.append(str(rel))
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--message', required=True)
    parser.add_argument('--paths', nargs='+', required=True)
    args = parser.parse_args()

    paths = existing_paths(args.paths)
    if not paths:
        print(json.dumps({'status': 'noop', 'reason': 'no_existing_paths'}))
        return

    run(['git', 'add', '--'] + paths)

    staged = subprocess.run(['git', 'diff', '--cached', '--quiet'], cwd=str(REPO_ROOT))
    if staged.returncode == 0:
        head = run(['git', 'rev-parse', 'HEAD']).stdout.strip()
        print(json.dumps({'status': 'noop', 'head': head, 'paths': paths}))
        return

    run(['git', 'commit', '-m', args.message])
    head = run(['git', 'rev-parse', 'HEAD']).stdout.strip()
    push = run(['git', 'push', 'origin', 'main'])
    print(json.dumps({'status': 'pushed', 'commit': head, 'paths': paths, 'push_stdout': push.stdout, 'push_stderr': push.stderr}, indent=2))


if __name__ == '__main__':
    main()

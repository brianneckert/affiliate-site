#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SITE_ROOT = ROOT.parent
POLICY_PATH = SITE_ROOT / 'config' / 'automation_policy.json'
CONTINUOUS_RUNS_PATH = SITE_ROOT / 'data' / 'analytics' / 'continuous_runs.json'
ALERTS_PATH = SITE_ROOT / 'data' / 'analytics' / 'alerts.json'
LOCKFILE_PATH = SITE_ROOT / 'data' / 'analytics' / 'continuous_batch.lock'
SYNC_SCRIPT = ROOT / 'sync_live_repo.py'
SITEMAP_SCRIPT = ROOT / 'generate_sitemap.py'


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_json(path, default):
    return json.loads(path.read_text()) if path.exists() else default


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def load_policy():
    return json.loads(POLICY_PATH.read_text())


def append_run_log(record):
    runs = load_json(CONTINUOUS_RUNS_PATH, [])
    runs.append(record)
    write_json(CONTINUOUS_RUNS_PATH, runs)


def append_alert_log(record):
    alerts = load_json(ALERTS_PATH, [])
    alerts.append(record)
    write_json(ALERTS_PATH, alerts)


def pid_is_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_lock(run_id, batch_date, batch_size):
    LOCKFILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if LOCKFILE_PATH.exists():
        try:
            lock = json.loads(LOCKFILE_PATH.read_text())
        except Exception:
            lock = {}
        pid = lock.get('pid')
        if pid and pid_is_alive(pid):
            skip_record = {
                'run_id': run_id,
                'mode': 'single_continuous_batch',
                'date': batch_date,
                'start_time': now_iso(),
                'end_time': now_iso(),
                'batch_size_requested': batch_size,
                'topics_processed': 0,
                'articles_processed': 0,
                'generated': 0,
                'validated': 0,
                'queued': 0,
                'published': 0,
                'failed': 0,
                'skipped': 1,
                'failure_reasons': [],
                'article_slugs_attempted': [],
                'article_slugs_queued': [],
                'article_slugs_published': [],
                'status': 'skipped_due_to_active_lock',
                'active_lock': lock
            }
            append_run_log(skip_record)
            print(json.dumps(skip_record, indent=2))
            raise SystemExit(0)
    LOCKFILE_PATH.write_text(json.dumps({'run_id': run_id, 'pid': os.getpid(), 'started_at': now_iso()}))


def release_lock():
    try:
        if LOCKFILE_PATH.exists():
            lock = json.loads(LOCKFILE_PATH.read_text())
            if lock.get('pid') == os.getpid():
                LOCKFILE_PATH.unlink()
    except Exception:
        pass


def run_json(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def recent_zero_generation_streak():
    rows = load_json(CONTINUOUS_RUNS_PATH, [])
    streak = []
    for row in reversed(rows):
        if row.get('mode') != 'single_continuous_batch':
            continue
        if row.get('status') == 'skipped_due_to_active_lock':
            continue
        if row.get('skipped', 0) != 0:
            break
        if row.get('generated') == 0:
            streak.append(row)
            if len(streak) >= 3:
                return list(reversed(streak[:3]))
            continue
        break
    return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default=str(date.today()))
    parser.add_argument('--batch-size', type=int, default=10)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--allow-publish', action='store_true')
    args = parser.parse_args()

    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    start_time = now_iso()
    acquire_lock(run_id, args.date, args.batch_size)

    try:
        if not 8 <= args.batch_size <= 12:
            raise SystemExit('batch-size must be between 8 and 12 for continuous mode')

        policy = load_policy()
        if not policy.get('auto_generate_enabled', True):
            raise SystemExit('auto_generate_enabled is false in policy')

        if policy.get('topic_discovery_enabled', True):
            topic_plan = run_json([
                'python3',
                str(ROOT / 'continuous_planner.py'),
                '--date',
                args.date,
                '--batch-size',
                str(min(args.batch_size, policy.get('max_articles_generated_per_day', args.batch_size)))
            ])
            write_json(SITE_ROOT / 'data' / 'analytics' / 'topic_plan.json', topic_plan)
        else:
            topic_plan = {'date': args.date, 'selected_topics': []}

        batch_cmd = [
            'python3',
            str(ROOT / 'batch_generate.py'),
            '--limit-topics',
            str(policy.get('max_topics_per_day', 6)),
            '--limit-articles',
            str(min(args.batch_size, policy.get('max_articles_generated_per_day', args.batch_size)))
        ]
        if args.dry_run:
            batch_cmd.append('--dry-run')
        batch_result = run_json(batch_cmd)

        promoted = []
        if args.allow_publish and not args.dry_run and policy.get('auto_publish_enabled', False):
            for item in batch_result.get('results', []):
                if item.get('status') == 'ready_to_publish':
                    promote = run_json(['python3', str(ROOT / 'autonomous_execute.py'), '--promote', item['slug'], '--live', '--no-sync'])
                    promoted.append(promote.get('article_slug'))
            if promoted:
                subprocess.run(['python3', str(SITEMAP_SCRIPT)], cwd=str(SITE_ROOT), check=True)
                sync_paths = ['data/articles/registry.json', 'sitemap.xml'] + [f'data/articles/{slug}' for slug in promoted]
                subprocess.run(['python3', str(SYNC_SCRIPT), '--message', f'publish: {len(promoted)} article(s)', '--paths', *sync_paths], cwd=str(SITE_ROOT), check=True)

        hold_seconds = int(os.environ.get('HOLD_LOCK_SECONDS', '0') or '0')
        if hold_seconds > 0:
            time.sleep(hold_seconds)

        if os.environ.get('SIMULATE_ZERO_GENERATED') == '1':
            batch_result = {
                'topic_plan_date': topic_plan.get('date'),
                'topics_processed': 0,
                'articles_processed': 0,
                'generated': 0,
                'queued': 0,
                'failed': 0,
                'skipped': 0,
                'results': []
            }
            promoted = []

        attempted = [item.get('slug') for item in batch_result.get('results', [])]
        queued = [item.get('slug') for item in batch_result.get('results', []) if item.get('status') == 'ready_to_publish' and item.get('slug') not in promoted]
        failures = [item for item in batch_result.get('results', []) if item.get('status') == 'failed']
        skipped = [item for item in batch_result.get('results', []) if item.get('status') == 'skipped']
        validated = batch_result.get('generated', 0)
        end_time = now_iso()

        log_record = {
            'run_id': run_id,
            'mode': 'single_continuous_batch',
            'date': args.date,
            'start_time': start_time,
            'end_time': end_time,
            'batch_size_requested': args.batch_size,
            'topics_processed': batch_result.get('topics_processed', 0),
            'articles_processed': batch_result.get('articles_processed', 0),
            'generated': batch_result.get('generated', 0),
            'validated': validated,
            'queued': len(queued),
            'published': len(promoted),
            'failed': len(failures),
            'skipped': len(skipped),
            'failure_reasons': [{'slug': item.get('slug'), 'reason': item.get('reason')} for item in failures],
            'article_slugs_attempted': attempted,
            'article_slugs_queued': queued,
            'article_slugs_published': promoted
        }
        append_run_log(log_record)

        zero_streak = recent_zero_generation_streak()
        alert_record = None
        if len(zero_streak) >= 3:
            log_record['status'] = 'alert_generation_stalled'
            runs = load_json(CONTINUOUS_RUNS_PATH, [])
            runs[-1] = log_record
            write_json(CONTINUOUS_RUNS_PATH, runs)
            alert_record = {
                'timestamp': now_iso(),
                'status': 'alert_generation_stalled',
                'consecutive_zero_runs': len(zero_streak),
                'last_run_ids': [row['run_id'] for row in zero_streak]
            }
            append_alert_log(alert_record)

        summary = {
            'run_id': run_id,
            'mode': 'single_continuous_batch',
            'date': args.date,
            'batch_size_requested': args.batch_size,
            'policy_inputs': {
                'max_topics_per_day': policy.get('max_topics_per_day'),
                'max_articles_generated_per_day': policy.get('max_articles_generated_per_day'),
                'max_articles_published_per_day': policy.get('max_articles_published_per_day'),
                'auto_generate_enabled': policy.get('auto_generate_enabled'),
                'auto_publish_enabled': policy.get('auto_publish_enabled'),
                'topic_discovery_enabled': policy.get('topic_discovery_enabled')
            },
            'topic_plan_date': topic_plan.get('date'),
            'batch_result': batch_result,
            'published_in_this_batch': promoted,
            'single_run_only': True,
            'log_path': str(CONTINUOUS_RUNS_PATH)
        }
        if alert_record:
            summary['status'] = 'alert_generation_stalled'
            summary['alert'] = alert_record
        print(json.dumps(summary, indent=2))
    finally:
        release_lock()


if __name__ == '__main__':
    main()

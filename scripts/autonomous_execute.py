#!/usr/bin/env python3
import argparse
import json
import subprocess
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

SITE_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = SITE_ROOT.parent
AFFILIATE_OS_ROOT = WORKSPACE_ROOT / 'affiliate_os'

POLICY_PATH = SITE_ROOT / 'config' / 'automation_policy.json'
DECISIONS_PATH = SITE_ROOT / 'data' / 'analytics' / 'decisions.json'
REGISTRY_PATH = SITE_ROOT / 'data' / 'articles' / 'registry.json'
EXECUTION_LOG_PATH = SITE_ROOT / 'data' / 'analytics' / 'execution_log.json'
SYNC_SCRIPT = SITE_ROOT / 'scripts' / 'sync_live_repo.py'
SITEMAP_SCRIPT = SITE_ROOT / 'scripts' / 'generate_sitemap.py'

BASE_AIR_PURIFIER_DATASET = AFFILIATE_OS_ROOT / 'data' / 'samples' / 'air_purifiers.json'
BASE_AIR_PURIFIER_WORKFLOW = AFFILIATE_OS_ROOT / 'workflows' / 'air_purifier_test.yaml'
AIR_PURIFIER_MAPPING = AFFILIATE_OS_ROOT / 'data' / 'amazon_mapping' / 'air_purifiers.json'


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_json(path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text())


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def append_execution_log(entry):
    log = load_json(EXECUTION_LOG_PATH, [])
    log.append(entry)
    write_json(EXECUTION_LOG_PATH, log)


def load_policy():
    return load_json(POLICY_PATH, {})


def load_decisions():
    return load_json(DECISIONS_PATH, {'recommended_actions': []})


def load_registry():
    return load_json(REGISTRY_PATH, {'articles': []})


def save_registry(registry):
    write_json(REGISTRY_PATH, registry)


def sync_related_articles(registry, category):
    family = [item for item in registry.get('articles', []) if item.get('category') == category]
    published = [item['article_slug'] for item in family if item.get('publish_status') == 'published']
    for article in family:
        if article.get('publish_status') == 'published':
            article['related_articles'] = [slug for slug in published if slug != article['article_slug']]
        else:
            article['related_articles'] = published[:]


def published_or_generated_slugs(registry):
    return {item['article_slug'] for item in registry.get('articles', [])}


def next_follow_on_plan(category, registry, policy):
    strategies = (policy.get('follow_on_strategies') or {}).get(category, [])
    existing = published_or_generated_slugs(registry)
    for strategy in strategies:
        if strategy['slug'] not in existing:
            return strategy
    return None


def build_air_purifier_follow_on_dataset(plan):
    base = load_json(BASE_AIR_PURIFIER_DATASET, {'products': []})
    products = []
    focus = plan.get('focus')
    focus_overrides = {
        'allergies': {
            'Coway Airmega AP-1512HH': {
                'best_for': 'Allergy relief in medium rooms',
                'notes': 'Balanced purifier with strong everyday filtration and dependable long-term value for allergy-prone homes.',
                'criteria': {'filtration_performance': 9, 'noise_control': 8, 'room_coverage': 7, 'long_term_value': 9}
            },
            'LEVOIT Core 300-P': {
                'best_for': 'Bedroom allergy control on a budget',
                'notes': 'Compact purifier that is easy to place in bedrooms where allergy control and quiet operation matter most.',
                'criteria': {'filtration_performance': 8, 'noise_control': 9, 'room_coverage': 7, 'long_term_value': 9}
            },
            'Blueair Blue Pure 311i Max': {
                'best_for': 'Large-room allergy filtration with smart controls',
                'notes': 'Strong mainstream allergy-focused option for larger rooms with smart controls and broad coverage.',
                'criteria': {'filtration_performance': 9, 'noise_control': 8, 'room_coverage': 9, 'long_term_value': 8}
            },
            'Winix 5510': {
                'best_for': 'Large-room allergen control with practical value',
                'notes': 'Large-room purifier that balances filtration strength and price for households managing allergens.',
                'criteria': {'filtration_performance': 8, 'noise_control': 8, 'room_coverage': 9, 'long_term_value': 8}
            },
            'GermGuardian AC4825E': {
                'best_for': 'Entry-level allergy filtering in smaller spaces',
                'notes': 'Affordable starting point for allergy control where low upfront cost matters more than maximum room coverage.',
                'criteria': {'filtration_performance': 7, 'noise_control': 8, 'room_coverage': 6, 'long_term_value': 9}
            }
        },
        'pets': {
            'Coway Airmega AP-1512HH': {
                'best_for': 'Pet dander control with balanced value',
                'notes': 'Reliable everyday purifier for pet households that want strong filtration without moving into premium pricing.',
                'criteria': {'filtration_performance': 9, 'noise_control': 8, 'room_coverage': 7, 'long_term_value': 9}
            },
            'LEVOIT Core 300-P': {
                'best_for': 'Pet hair and odor control in bedrooms',
                'notes': 'Compact purifier that fits smaller rooms where pet hair, dander, and overnight noise matter.',
                'criteria': {'filtration_performance': 8, 'noise_control': 9, 'room_coverage': 7, 'long_term_value': 9}
            },
            'Blueair Blue Pure 311i Max': {
                'best_for': 'Large pet-friendly living spaces',
                'notes': 'Strong large-room option for households managing pet dander across open living areas.',
                'criteria': {'filtration_performance': 9, 'noise_control': 8, 'room_coverage': 9, 'long_term_value': 8}
            },
            'Winix 5510': {
                'best_for': 'Pet households needing strong deodorization',
                'notes': 'Practical large-room purifier with strong value for homes dealing with pet odors and airborne dander.',
                'criteria': {'filtration_performance': 8, 'noise_control': 8, 'room_coverage': 9, 'long_term_value': 8}
            },
            'GermGuardian AC4825E': {
                'best_for': 'Entry-level purifier for pet owners on a budget',
                'notes': 'Affordable option for smaller pet spaces where odor reduction and lower cost matter most.',
                'criteria': {'filtration_performance': 7, 'noise_control': 8, 'room_coverage': 6, 'long_term_value': 9}
            }
        },
        'large rooms': {
            'Coway Airmega AP-1512HH': {
                'best_for': 'Medium-to-large rooms with strong all-around value',
                'notes': 'Still a balanced value choice, but not the strongest coverage option in this large-room-focused set.',
                'criteria': {'filtration_performance': 8, 'noise_control': 8, 'room_coverage': 7, 'long_term_value': 9}
            },
            'LEVOIT Core 300-P': {
                'best_for': 'Smaller large-room budgets',
                'notes': 'Affordable and quiet, but coverage is less convincing than the strongest large-room leaders here.',
                'criteria': {'filtration_performance': 7, 'noise_control': 9, 'room_coverage': 6, 'long_term_value': 9}
            },
            'Blueair Blue Pure 311i Max': {
                'best_for': 'Best large-room smart purification',
                'notes': 'Best overall fit here for large-room purification with strong coverage and easy smart controls.',
                'criteria': {'filtration_performance': 9, 'noise_control': 8, 'room_coverage': 9, 'long_term_value': 8}
            },
            'Winix 5510': {
                'best_for': 'Large rooms on a more practical budget',
                'notes': 'Very strong large-room option for buyers who want serious coverage without moving up to pricier models.',
                'criteria': {'filtration_performance': 8, 'noise_control': 8, 'room_coverage': 9, 'long_term_value': 9}
            },
            'GermGuardian AC4825E': {
                'best_for': 'Budget fallback for modest spaces',
                'notes': 'Useful on price, but clearly the weakest fit for truly large-room purification.',
                'criteria': {'filtration_performance': 6, 'noise_control': 7, 'room_coverage': 5, 'long_term_value': 8}
            }
        },
        'quiet operation': {
            'Coway Airmega AP-1512HH': {
                'best_for': 'Quiet all-around performance',
                'notes': 'Balanced quieter option with dependable value and solid everyday filtration.',
                'criteria': {'filtration_performance': 8, 'noise_control': 9, 'room_coverage': 7, 'long_term_value': 9}
            },
            'LEVOIT Core 300-P': {
                'best_for': 'Best quiet bedroom value',
                'notes': 'The strongest quiet-operation value play for bedrooms and smaller spaces.',
                'criteria': {'filtration_performance': 8, 'noise_control': 10, 'room_coverage': 7, 'long_term_value': 9}
            },
            'Blueair Blue Pure 311i Max': {
                'best_for': 'Quiet large-room smart use',
                'notes': 'Strong option when you want quieter operation without giving up large-room capability.',
                'criteria': {'filtration_performance': 9, 'noise_control': 9, 'room_coverage': 9, 'long_term_value': 8}
            },
            'Winix 5510': {
                'best_for': 'Quiet auto-mode large-room coverage',
                'notes': 'Good quiet-mode value for larger rooms, though not the softest-sounding option in every setting.',
                'criteria': {'filtration_performance': 8, 'noise_control': 8, 'room_coverage': 9, 'long_term_value': 8}
            },
            'GermGuardian AC4825E': {
                'best_for': 'Low-cost quiet-enough starter option',
                'notes': 'Affordable, but quieter-operation shoppers will usually prefer stronger bedroom-focused models above it.',
                'criteria': {'filtration_performance': 7, 'noise_control': 7, 'room_coverage': 6, 'long_term_value': 9}
            }
        }
    }
    overrides = focus_overrides[focus]
    for product in base['products']:
        item = deepcopy(product)
        if item['name'] in overrides:
            item.update(overrides[item['name']])
        products.append(item)
    return {'products': products}


def build_air_purifier_follow_on_workflow(plan):
    focus = plan.get('focus')
    search_phrase = f"best air purifiers for {focus}" if focus not in {'quiet operation', 'budget under $200'} else (
        'best quiet air purifiers' if focus == 'quiet operation' else 'best air purifiers under $200'
    )
    objective_focus = plan['title'].lower()
    buying_guide = [
        'Prioritize filtration performance and realistic room sizing for the use case this guide targets.',
        'Noise control matters more when the purifier will run in bedrooms, nurseries, or workspaces.',
        'Filter cost and upkeep affect long-term value more than sticker price alone.',
        'Smart features can help, but clean-air performance should still drive the final choice.'
    ]
    faq = [
        {
            'question': f"What matters most when choosing an air purifier for {focus}?",
            'answer': 'The best choice usually balances filtration performance, room fit, day-to-day noise, and realistic long-term ownership cost.'
        },
        {
            'question': 'Should you choose based only on the biggest coverage number?',
            'answer': 'No. Coverage claims matter, but actual room size, noise tolerance, and how often the purifier runs matter just as much.'
        }
    ]
    verdict = {
        'allergies': 'If allergy control is the main goal, choose {top_pick} from this validated Amazon-only test set.',
        'pets': 'If pet dander and odor control matter most, choose {top_pick} from this validated Amazon-only test set.',
        'large rooms': 'If you need stronger large-room purification, choose {top_pick} from this validated Amazon-only test set.',
        'quiet operation': 'If low-noise performance matters most, choose {top_pick} from this validated Amazon-only test set.'
    }.get(focus, 'If this is your target use case, choose {top_pick} from this validated Amazon-only test set.')

    return {
        'workflow': plan['slug'].replace('-', '_'),
        'article': {
            'slug': plan['slug'],
            'category': 'air purifiers',
            'search_phrase': search_phrase,
            'article_title': plan['title'],
            'objective': f'Generate a structured affiliate article for {objective_focus} using the configured offline dataset only.',
            'scoring_dimensions': ['filtration performance', 'noise control', 'room coverage', 'long-term value'],
            'buying_guide': buying_guide,
            'faq': faq,
            'summary_template': '{top_pick} is the top pick based on weighted scoring across {scoring_phrase}.',
            'final_verdict_template': verdict
        },
        'dataset': f"data/samples/{plan['slug'].replace('-', '_')}.json",
        'amazon_mapping_dataset': 'data/amazon_mapping/air_purifiers.json',
        'output_dir': f"runs/manual_tests/{plan['slug'].replace('-', '_')}",
        'stages': [
            {'name': 'Orchestrator', 'agent': 'Orchestrator'},
            {'name': 'Research', 'agent': 'Research'},
            {
                'name': 'ProductIntelligence',
                'agent': 'ProductIntelligence',
                'config': {
                    'weights': {
                        'filtration_performance': 1.0,
                        'noise_control': 0.8,
                        'room_coverage': 1.0,
                        'long_term_value': 1.0
                    }
                }
            },
            {'name': 'ContentProduction', 'agent': 'ContentProduction'},
            {'name': 'Compliance', 'agent': 'Compliance'}
        ]
    }


def promote_article_to_published(article_slug, dry_run=False, sync_repo=True):
    registry = load_registry()
    article = next((item for item in registry.get('articles', []) if item.get('article_slug') == article_slug), None)
    if not article:
        raise SystemExit(f'Unknown article slug: {article_slug}')
    article_dir = SITE_ROOT / article['article_dir']
    required = [article_dir / 'productintelligence.json', article_dir / 'contentproduction.json', article_dir / 'compliance.json']
    if not all(path.exists() for path in required):
        raise SystemExit(f'Cannot promote {article_slug}: missing article bundle files in {article_dir}')

    content = load_json(article_dir / 'contentproduction.json', {})
    compliance = load_json(article_dir / 'compliance.json', {})
    intelligence = load_json(article_dir / 'productintelligence.json', {})
    validation_ok = (
        article.get('validation_result', {}).get('passed', True) is True
        and compliance.get('passed') is True
        and len(content.get('comparison', [])) == 5
        and len(intelligence.get('products', [])) == 5
        and all(bool(p.get('affiliate_url')) for p in intelligence.get('products', []))
    )
    if not validation_ok:
        raise SystemExit(f'Cannot promote {article_slug}: validation/compliance checks failed')

    if dry_run:
        return {
            'mode': 'promote-dry-run',
            'article_slug': article_slug,
            'would_set_publish_status': 'published',
            'would_set_published_at': now_iso()
        }
    previous_status = article.get('publish_status')
    previous_published_at = article.get('published_at')
    article['publish_status'] = 'published'
    article['published_at'] = now_iso()
    sync_related_articles(registry, article.get('category'))
    save_registry(registry)
    result = {
        'mode': 'promote-live',
        'article_slug': article_slug,
        'publish_status': article['publish_status'],
        'published_at': article['published_at']
    }
    if sync_repo:
        try:
            result['repo_sync'] = sync_live_repo([article['article_dir']], f'publish: 1 article ({article_slug})')
        except Exception:
            article['publish_status'] = previous_status
            article['published_at'] = previous_published_at
            sync_related_articles(registry, article.get('category'))
            save_registry(registry)
            subprocess.run(['python3', str(SITEMAP_SCRIPT)], cwd=str(SITE_ROOT), check=False)
            raise
    return result




def sync_live_repo(paths, message):
    subprocess.run(['python3', str(SITEMAP_SCRIPT)], cwd=str(SITE_ROOT), check=True)
    sync_paths = list(dict.fromkeys(list(paths) + ['data/articles/registry.json', 'sitemap.xml']))
    result = subprocess.run(
        ['python3', str(SYNC_SCRIPT), '--message', message, '--paths', *sync_paths],
        cwd=str(SITE_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def create_follow_on_article(plan, policy, dry_run=False):
    slug = plan['slug']
    dataset_filename = slug.replace('-', '_') + '.json'
    workflow_filename = slug.replace('-', '_') + '.yaml'
    dataset_path = AFFILIATE_OS_ROOT / 'data' / 'samples' / dataset_filename
    workflow_path = AFFILIATE_OS_ROOT / 'workflows' / workflow_filename
    output_dir = AFFILIATE_OS_ROOT / 'runs' / 'manual_tests' / slug.replace('-', '_')
    article_dir = SITE_ROOT / 'data' / 'articles' / slug

    if dry_run:
        return {
            'mode': 'dry-run',
            'article_slug': slug,
            'would_create_files': [str(dataset_path), str(workflow_path), str(article_dir / 'productintelligence.json'), str(article_dir / 'contentproduction.json'), str(article_dir / 'compliance.json')],
            'would_publish': False
        }

    dataset_payload = build_air_purifier_follow_on_dataset(plan)
    workflow_payload = build_air_purifier_follow_on_workflow(plan)
    write_json(dataset_path, dataset_payload)
    write_json(workflow_path, workflow_payload)

    subprocess.run(
        ['python3', str(AFFILIATE_OS_ROOT / 'scripts' / 'run_workflow.py'), '--workflow', f'workflows/{workflow_filename}'],
        cwd=str(AFFILIATE_OS_ROOT),
        check=True,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    article_dir.mkdir(parents=True, exist_ok=True)
    for name in ['productintelligence.json', 'contentproduction.json', 'compliance.json']:
        (article_dir / name).write_text((output_dir / name).read_text())

    content = load_json(article_dir / 'contentproduction.json', {})
    compliance = load_json(article_dir / 'compliance.json', {})
    intelligence = load_json(article_dir / 'productintelligence.json', {})
    valid = (
        compliance.get('passed') is True
        and len(content.get('comparison', [])) == 5
        and len(intelligence.get('products', [])) == 5
        and all(bool(p.get('affiliate_url')) for p in intelligence.get('products', []))
    )
    if not valid:
        raise SystemExit('Generated follow-on article failed validation checks.')

    registry = load_registry()
    registry['articles'].append({
        'article_slug': slug,
        'category': 'air purifiers',
        'title': plan['title'],
        'workflow_path': str(workflow_path),
        'dataset_path': str(dataset_path),
        'amazon_mapping_dataset_path': str(AIR_PURIFIER_MAPPING),
        'output_dir': str(output_dir),
        'article_dir': f'data/articles/{slug}',
        'publish_status': 'ready_to_publish',
        'published_at': None,
        'source_article_family': 'air purifiers',
        'related_articles': []
    })
    sync_related_articles(registry, 'air purifiers')
    save_registry(registry)

    result = {
        'mode': 'live-prep',
        'article_slug': slug,
        'created_files': [str(dataset_path), str(workflow_path), str(article_dir / 'productintelligence.json'), str(article_dir / 'contentproduction.json'), str(article_dir / 'compliance.json')],
        'published': False,
        'publish_status': 'ready_to_publish'
    }
    if policy.get('auto_publish_enabled'):
        promoted = promote_article_to_published(slug, dry_run=False, sync_repo=True)
        result['published'] = True
        result['publish_status'] = promoted['publish_status']
        result['repo_sync'] = promoted.get('repo_sync')
    return result


def run_default_decision_loop(policy, decisions, registry):
    dry_run = policy.get('dry_run', True)
    approved_categories = set(policy.get('approved_categories', []))
    actions = decisions.get('recommended_actions', [])
    selected = None
    for action in actions:
        if action.get('action_type') == 'create_more_articles_in_category' and action.get('target') in approved_categories and float(action.get('confidence', 0)) >= float(policy.get('min_confidence_required', 0.7)):
            selected = action
            break
    if selected:
        plan = next_follow_on_plan(selected['target'], registry, policy)
        if not plan:
            return {'mode': 'decision-loop', 'result': 'no_available_follow_on_plan'}
        return {
            'mode': 'decision-loop',
            'decision': selected,
            'next_article_plan': plan,
            'would_publish': bool(policy.get('auto_publish_enabled')) and not dry_run
        }
    return {
        'mode': 'decision-loop',
        'result': 'keep_collecting_data',
        'would_publish': False,
        'next_article_plan': next_follow_on_plan('air purifiers', registry, policy)
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prepare-follow-on', choices=['air purifiers'])
    parser.add_argument('--promote')
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--no-sync', action='store_true')
    args = parser.parse_args()

    policy = load_policy()
    decisions = load_decisions()
    registry = load_registry()

    if args.prepare_follow_on:
        plan = next_follow_on_plan(args.prepare_follow_on, registry, policy)
        if not plan:
            result = {'mode': 'follow-on-prep', 'result': 'no_available_plan'}
        else:
            result = create_follow_on_article(plan, policy, dry_run=not args.live)
        append_execution_log({
            'timestamp': now_iso(),
            'action_attempted': 'create_more_articles_in_category',
            'target_category': args.prepare_follow_on,
            'target_article_slug': result.get('article_slug'),
            'dry_run': not args.live,
            'result': result.get('publish_status') or result.get('result') or 'prepared',
            'error': None
        })
        print(json.dumps(result, indent=2))
        return

    if args.promote:
        result = promote_article_to_published(args.promote, dry_run=not args.live, sync_repo=not args.no_sync)
        append_execution_log({
            'timestamp': now_iso(),
            'action_attempted': 'promote_article_to_published',
            'target_category': next((item.get('category') for item in registry.get('articles', []) if item.get('article_slug') == args.promote), None),
            'target_article_slug': args.promote,
            'dry_run': not args.live,
            'result': result.get('publish_status') or 'promoted',
            'error': None
        })
        print(json.dumps(result, indent=2))
        return

    result = run_default_decision_loop(policy, decisions, registry)
    append_execution_log({
        'timestamp': now_iso(),
        'action_attempted': result.get('decision', {}).get('action_type', 'keep_collecting_data'),
        'target_category': result.get('decision', {}).get('target') if result.get('decision') else 'air purifiers',
        'target_article_slug': result.get('next_article_plan', {}).get('slug') if result.get('next_article_plan') else None,
        'dry_run': policy.get('dry_run', True),
        'result': result.get('result', 'would_plan_follow_on' if result.get('next_article_plan') else 'noop'),
        'error': None
    })
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()

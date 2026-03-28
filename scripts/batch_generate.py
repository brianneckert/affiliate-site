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
TOPIC_PLAN_PATH = SITE_ROOT / 'data' / 'analytics' / 'topic_plan.json'
REGISTRY_PATH = SITE_ROOT / 'data' / 'articles' / 'registry.json'
POLICY_PATH = SITE_ROOT / 'config' / 'automation_policy.json'
SUPPORTED_PATH = SITE_ROOT / 'config' / 'supported_topic_families.json'
EXECUTION_LOG_PATH = SITE_ROOT / 'data' / 'analytics' / 'execution_log.json'

BASE_DATASETS = {
    'air purifiers': AFFILIATE_OS_ROOT / 'data' / 'samples' / 'air_purifiers.json',
    'espresso grinders': AFFILIATE_OS_ROOT / 'data' / 'samples' / 'espresso_grinders.json',
    'electric toothbrushes': AFFILIATE_OS_ROOT / 'data' / 'samples' / 'electric_toothbrushes.json',
    'air fryers': AFFILIATE_OS_ROOT / 'data' / 'samples' / 'air_fryers.json',
    'blenders': AFFILIATE_OS_ROOT / 'data' / 'samples' / 'blenders.json',
    'electric razors': AFFILIATE_OS_ROOT / 'data' / 'samples' / 'electric_razors.json'
}
MAPPING_PATHS = {
    'air purifiers': AFFILIATE_OS_ROOT / 'data' / 'amazon_mapping' / 'air_purifiers.json',
    'espresso grinders': AFFILIATE_OS_ROOT / 'data' / 'amazon_mapping' / 'espresso_grinders.json',
    'electric toothbrushes': AFFILIATE_OS_ROOT / 'data' / 'amazon_mapping' / 'electric_toothbrushes.json',
    'air fryers': AFFILIATE_OS_ROOT / 'data' / 'amazon_mapping' / 'air_fryers.json',
    'blenders': AFFILIATE_OS_ROOT / 'data' / 'amazon_mapping' / 'blenders.json',
    'electric razors': AFFILIATE_OS_ROOT / 'data' / 'amazon_mapping' / 'electric_razors.json'
}


def load_json(path, default):
    if not Path(path).exists():
        return default
    return json.loads(Path(path).read_text())


def write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def append_log(entry):
    log = load_json(EXECUTION_LOG_PATH, [])
    log.append(entry)
    write_json(EXECUTION_LOG_PATH, log)


def sync_related_articles(registry, category):
    fam = [a for a in registry.get('articles', []) if a.get('category') == category]
    published = [a['article_slug'] for a in fam if a.get('publish_status') == 'published']
    for article in fam:
      if article.get('publish_status') == 'published':
        article['related_articles'] = [slug for slug in published if slug != article['article_slug']]
      else:
        article['related_articles'] = published[:]


def make_dataset(topic_family, family_position, article_slug):
    base = load_json(BASE_DATASETS[topic_family], {'products': []})
    products = []
    if topic_family == 'air purifiers':
        allowed = {item['name'] for item in base['products']}
        overrides = {
            'main': {},
            'budget': {},
            'bedrooms': {}
        }
    elif topic_family == 'espresso grinders':
        allowed = {'Baratza Encore ESP','Breville Smart Grinder Pro','Fellow Opus','Eureka Mignon Specialita','Eureka Mignon Silenzio'}
        overrides = {
            'main': {},
            'beginners': {
                'Baratza Encore ESP': {'best_for': 'New home espresso users', 'criteria': {'espresso_focus': 8,'ease_of_use': 9,'build_quality': 8,'value_retention': 8}},
                'Breville Smart Grinder Pro': {'best_for': 'Beginners who want presets and convenience', 'criteria': {'espresso_focus': 7,'ease_of_use': 9,'build_quality': 7,'value_retention': 7}},
                'Fellow Opus': {'best_for': 'Beginners who want stylish value', 'criteria': {'espresso_focus': 8,'ease_of_use': 8,'build_quality': 7,'value_retention': 8}},
                'Eureka Mignon Specialita': {'best_for': 'Beginners ready to spend more for long-term quality', 'criteria': {'espresso_focus': 9,'ease_of_use': 7,'build_quality': 9,'value_retention': 8}},
                'Eureka Mignon Silenzio': {'best_for': 'Beginners who care about quieter use', 'criteria': {'espresso_focus': 8,'ease_of_use': 8,'build_quality': 8,'value_retention': 8}}
            },
            'small_kitchens': {
                'Baratza Encore ESP': {'best_for': 'Compact espresso setups', 'criteria': {'espresso_focus': 8,'ease_of_use': 8,'build_quality': 8,'value_retention': 8}},
                'Breville Smart Grinder Pro': {'best_for': 'Counter-friendly all-in-one convenience', 'criteria': {'espresso_focus': 7,'ease_of_use': 9,'build_quality': 7,'value_retention': 7}},
                'Fellow Opus': {'best_for': 'Small kitchens with design-conscious setups', 'criteria': {'espresso_focus': 8,'ease_of_use': 8,'build_quality': 7,'value_retention': 8}},
                'Eureka Mignon Specialita': {'best_for': 'Premium small-footprint flat-burr grinding', 'criteria': {'espresso_focus': 9,'ease_of_use': 7,'build_quality': 9,'value_retention': 8}},
                'Eureka Mignon Silenzio': {'best_for': 'Quieter small kitchens', 'criteria': {'espresso_focus': 8,'ease_of_use': 8,'build_quality': 8,'value_retention': 8}}
            },
            'budget': {
                'Baratza Encore ESP': {'best_for': 'Best value espresso-focused pick', 'criteria': {'espresso_focus': 8,'ease_of_use': 8,'build_quality': 8,'value_retention': 9}},
                'Breville Smart Grinder Pro': {'best_for': 'Budget shoppers who want more convenience features', 'criteria': {'espresso_focus': 7,'ease_of_use': 9,'build_quality': 7,'value_retention': 8}},
                'Fellow Opus': {'best_for': 'Budget-conscious buyers who still want style', 'criteria': {'espresso_focus': 8,'ease_of_use': 8,'build_quality': 7,'value_retention': 9}},
                'Eureka Mignon Specialita': {'best_for': 'Stretch-budget premium buyers', 'criteria': {'espresso_focus': 9,'ease_of_use': 7,'build_quality': 9,'value_retention': 7}},
                'Eureka Mignon Silenzio': {'best_for': 'Budget-conscious buyers who value low noise', 'criteria': {'espresso_focus': 8,'ease_of_use': 8,'build_quality': 8,'value_retention': 8}}
            },
            'quiet': {
                'Baratza Encore ESP': {'best_for': 'Simple espresso grinding with fewer noise priorities', 'criteria': {'espresso_focus': 8,'ease_of_use': 8,'build_quality': 8,'value_retention': 8}},
                'Breville Smart Grinder Pro': {'best_for': 'Feature-heavy convenience with moderate noise', 'criteria': {'espresso_focus': 7,'ease_of_use': 9,'build_quality': 7,'value_retention': 7}},
                'Fellow Opus': {'best_for': 'Quiet-ish compact use', 'criteria': {'espresso_focus': 8,'ease_of_use': 8,'build_quality': 7,'value_retention': 8}},
                'Eureka Mignon Specialita': {'best_for': 'Premium quiet flat-burr grinding', 'criteria': {'espresso_focus': 9,'ease_of_use': 7,'build_quality': 9,'value_retention': 8}},
                'Eureka Mignon Silenzio': {'best_for': 'Best quiet-value espresso grinding', 'criteria': {'espresso_focus': 8,'ease_of_use': 8,'build_quality': 8,'value_retention': 9}}
            }
        }
    elif topic_family == 'electric toothbrushes':
        allowed = {'Oral-B Pro 1000','Oral-B iO Series 5','Philips Sonicare 4100','Philips Sonicare ExpertClean 7500','Aquasonic Black Series'}
        overrides = {
            'main': {},
            'sensitive_gums': {
                'Oral-B Pro 1000': {'best_for': 'Entry Oral-B buyers with gum-sensitivity awareness', 'criteria': {'cleaning_performance': 8,'comfort': 7,'battery_convenience': 7,'long_term_value': 9}},
                'Oral-B iO Series 5': {'best_for': 'Pressure-guided gum-sensitive brushing', 'criteria': {'cleaning_performance': 9,'comfort': 8,'battery_convenience': 8,'long_term_value': 7}},
                'Philips Sonicare 4100': {'best_for': 'Gentler sonic brushing for sensitive gums', 'criteria': {'cleaning_performance': 8,'comfort': 10,'battery_convenience': 8,'long_term_value': 9}},
                'Philips Sonicare ExpertClean 7500': {'best_for': 'Premium comfort-focused sonic cleaning', 'criteria': {'cleaning_performance': 9,'comfort': 9,'battery_convenience': 8,'long_term_value': 7}},
                'Aquasonic Black Series': {'best_for': 'Budget-friendly softer-feel sonic use', 'criteria': {'cleaning_performance': 7,'comfort': 8,'battery_convenience': 8,'long_term_value': 10}}
            },
            'travel': {
                'Oral-B Pro 1000': {'best_for': 'Simple travel brushing without premium extras', 'criteria': {'cleaning_performance': 8,'comfort': 7,'battery_convenience': 7,'long_term_value': 8}},
                'Oral-B iO Series 5': {'best_for': 'Premium travel with case included', 'criteria': {'cleaning_performance': 9,'comfort': 8,'battery_convenience': 8,'long_term_value': 7}},
                'Philips Sonicare 4100': {'best_for': 'Slim travel-friendly sonic brushing', 'criteria': {'cleaning_performance': 8,'comfort': 9,'battery_convenience': 8,'long_term_value': 9}},
                'Philips Sonicare ExpertClean 7500': {'best_for': 'Premium travel sonic use', 'criteria': {'cleaning_performance': 9,'comfort': 9,'battery_convenience': 9,'long_term_value': 7}},
                'Aquasonic Black Series': {'best_for': 'Best budget travel bundle', 'criteria': {'cleaning_performance': 7,'comfort': 8,'battery_convenience': 8,'long_term_value': 10}}
            },
            'budget': {
                'Oral-B Pro 1000': {'best_for': 'Best overall value Oral-B', 'criteria': {'cleaning_performance': 8,'comfort': 7,'battery_convenience': 7,'long_term_value': 10}},
                'Oral-B iO Series 5': {'best_for': 'Stretch-budget premium Oral-B buyers', 'criteria': {'cleaning_performance': 9,'comfort': 8,'battery_convenience': 8,'long_term_value': 6}},
                'Philips Sonicare 4100': {'best_for': 'Best budget-friendly sonic option', 'criteria': {'cleaning_performance': 8,'comfort': 9,'battery_convenience': 8,'long_term_value': 10}},
                'Philips Sonicare ExpertClean 7500': {'best_for': 'Premium budget stretch pick', 'criteria': {'cleaning_performance': 9,'comfort': 9,'battery_convenience': 8,'long_term_value': 6}},
                'Aquasonic Black Series': {'best_for': 'Maximum accessories per dollar', 'criteria': {'cleaning_performance': 7,'comfort': 8,'battery_convenience': 8,'long_term_value': 10}}
            },
            'smart': {
                'Oral-B Pro 1000': {'best_for': 'Buyers skipping smart extras', 'criteria': {'cleaning_performance': 8,'comfort': 7,'battery_convenience': 7,'long_term_value': 9}},
                'Oral-B iO Series 5': {'best_for': 'Best smart Oral-B upgrade', 'criteria': {'cleaning_performance': 9,'comfort': 8,'battery_convenience': 8,'long_term_value': 8}},
                'Philips Sonicare 4100': {'best_for': 'Simple sonic brushing without smart overhead', 'criteria': {'cleaning_performance': 8,'comfort': 9,'battery_convenience': 8,'long_term_value': 9}},
                'Philips Sonicare ExpertClean 7500': {'best_for': 'Premium feature-rich Sonicare users', 'criteria': {'cleaning_performance': 9,'comfort': 9,'battery_convenience': 8,'long_term_value': 7}},
                'Aquasonic Black Series': {'best_for': 'Budget buyers who want multiple modes, not app features', 'criteria': {'cleaning_performance': 7,'comfort': 8,'battery_convenience': 8,'long_term_value': 9}}
            }
        }
    else:
        allowed = {item['name'] for item in base['products']}
        overrides = {'main': {}}
    chosen = overrides.get(family_position, overrides['main'])
    for item in base['products']:
        if item['name'] not in allowed:
            continue
        row = deepcopy(item)
        if row['name'] in chosen:
            row.update(chosen[row['name']])
        products.append(row)
    return {'products': products}


def make_workflow(topic_family, article_slug, title, family_position):
    if topic_family == 'air purifiers':
        article = {
            'slug': article_slug,
            'category': 'air purifiers',
            'search_phrase': title.lower(),
            'article_title': title,
            'objective': f'Generate a structured affiliate article for {title.lower()} using the configured offline dataset only.',
            'scoring_dimensions': ['filtration performance', 'noise control', 'room coverage', 'long-term value'],
            'buying_guide': ['Prioritize filtration performance for smoke, dust, and allergies.', 'Noise matters more in bedrooms and offices.', 'Coverage claims should be read against real room size.', 'Long-term value includes filters and upkeep, not just sticker price.'],
            'faq': [
                {'question': 'Do you need a bigger purifier than your room size?', 'answer': 'Not necessarily, but stronger real-world coverage can help if you want quieter operation at lower fan speeds.'},
                {'question': 'Should buyers focus only on app features?', 'answer': 'No. Filtration performance, room fit, and long-term filter cost usually matter more.'}
            ],
            'summary_template': '{top_pick} is the top pick based on weighted scoring across {scoring_phrase}.',
            'final_verdict_template': 'If this air-purifier angle matches your needs, choose {top_pick} from this validated Amazon-only test set.'
        }
        dataset = f"data/samples/{article_slug.replace('-', '_')}.json"
        mapping = 'data/amazon_mapping/air_purifiers.json'
        weights = {'filtration_performance': 1.0, 'noise_control': 0.8, 'room_coverage': 1.0, 'long_term_value': 1.0}
    elif topic_family == 'espresso grinders':
        article = {
            'slug': article_slug,
            'category': 'espresso grinders',
            'search_phrase': title.lower(),
            'article_title': title,
            'objective': f'Generate a structured affiliate article for {title.lower()} using the configured offline dataset only.',
            'scoring_dimensions': ['espresso focus', 'ease of use', 'build quality', 'value retention'],
            'buying_guide': ['Match the grinder to your espresso routine, budget, and counter space.', 'Noise, workflow, and retention matter more than spec-sheet hype alone.', 'Flat vs conical burr tradeoffs should be weighed against actual use case.', 'Long-term value matters when you buy above entry level.'],
            'faq': [
                {'question': 'Do you need an espresso-focused grinder?', 'answer': 'If espresso is the main use case, tighter grind adjustment and better espresso consistency usually matter more than maximum versatility.'},
                {'question': 'Should budget buyers avoid premium grinders completely?', 'answer': 'Not necessarily, but budget-focused buyers should weigh workflow and value rather than assuming a higher price alone guarantees a better fit.'}
            ],
            'summary_template': '{top_pick} is the top pick based on weighted scoring across {scoring_phrase}.',
            'final_verdict_template': 'If this espresso-grinder angle matches your needs, choose {top_pick} from this validated Amazon-only test set.'
        }
        dataset = f"data/samples/{article_slug.replace('-', '_')}.json"
        mapping = 'data/amazon_mapping/espresso_grinders.json'
        weights = {'espresso_focus': 1.0, 'ease_of_use': 0.8, 'build_quality': 1.0, 'value_retention': 1.0}
    elif topic_family == 'electric toothbrushes':
        article = {
            'slug': article_slug,
            'category': 'electric toothbrushes',
            'search_phrase': title.lower(),
            'article_title': title,
            'objective': f'Generate a structured affiliate article for {title.lower()} using the configured offline dataset only.',
            'scoring_dimensions': ['cleaning performance', 'comfort', 'battery convenience', 'long-term value'],
            'buying_guide': ['Comfort and brushing style matter as much as the brand name.', 'Replacement-head cost changes long-term value more than buyers often expect.', 'Travel and charging convenience matter if the brush leaves the bathroom often.', 'Premium modes are nice, but most buyers benefit most from consistent daily use.'],
            'faq': [
                {'question': 'Is sonic or oscillating brushing automatically better?', 'answer': 'Not automatically. The best fit often depends on comfort preference, gum sensitivity, and how much premium complexity you actually want.'},
                {'question': 'Should budget buyers skip premium electric toothbrushes?', 'answer': 'Only if the extra modes, pressure guidance, or travel extras do not matter to the actual use case.'}
            ],
            'summary_template': '{top_pick} is the top pick based on weighted scoring across {scoring_phrase}.',
            'final_verdict_template': 'If this electric-toothbrush angle matches your needs, choose {top_pick} from this validated Amazon-only test set.'
        }
        dataset = f"data/samples/{article_slug.replace('-', '_')}.json"
        mapping = 'data/amazon_mapping/electric_toothbrushes.json'
        weights = {'cleaning_performance': 1.0, 'comfort': 0.8, 'battery_convenience': 1.0, 'long_term_value': 1.0}
    else:
        article = {
            'slug': article_slug,
            'category': topic_family,
            'search_phrase': title.lower(),
            'article_title': title,
            'objective': f'Generate a structured affiliate article for {title.lower()} using the configured offline dataset only.',
            'scoring_dimensions': ['core performance', 'usability', 'build quality', 'long-term value'],
            'buying_guide': [f'Prioritize the core strengths that matter most when shopping {topic_family}.', 'Ease of use and long-term value often matter as much as the headline feature list.', 'Compare real buyer use cases instead of relying only on spec-sheet marketing.', 'Budget, footprint, and maintenance should stay in the decision loop.'],
            'faq': [
                {'question': f'What matters most when shopping {topic_family}?', 'answer': 'The best pick usually balances performance, usability, quality, and long-term ownership value.'},
                {'question': 'Should budget buyers always avoid premium options?', 'answer': 'Not always. Premium options can make sense if the use case really needs the extra quality or convenience.'}
            ],
            'summary_template': '{top_pick} is the top pick based on weighted scoring across {scoring_phrase}.',
            'final_verdict_template': f'If this {topic_family} angle matches your needs, choose {{top_pick}} from this validated Amazon-only test set.'
        }
        dataset = f"data/samples/{article_slug.replace('-', '_')}.json"
        mapping = f"data/amazon_mapping/{topic_family.replace(' ', '_')}.json"
        weights = {'core_performance': 1.0, 'usability': 0.8, 'build_quality': 1.0, 'long_term_value': 1.0}
    return {
        'workflow': article_slug.replace('-', '_'),
        'article': article,
        'dataset': dataset,
        'amazon_mapping_dataset': mapping,
        'output_dir': f"runs/manual_tests/{article_slug.replace('-', '_')}",
        'stages': [
            {'name': 'Orchestrator', 'agent': 'Orchestrator'},
            {'name': 'Research', 'agent': 'Research'},
            {'name': 'ProductIntelligence', 'agent': 'ProductIntelligence', 'config': {'weights': weights}},
            {'name': 'ContentProduction', 'agent': 'ContentProduction'},
            {'name': 'Compliance', 'agent': 'Compliance'}
        ]
    }


def generate_article(topic, article, plan_date, dry_run):
    slug = article['article_slug']
    registry = load_json(REGISTRY_PATH, {'articles': []})
    supported = load_json(SUPPORTED_PATH, {'families': []})
    live_ready = {item['topic_family'] for item in supported.get('families', []) if item.get('status') == 'live-ready'}
    if any(item.get('article_slug') == slug for item in registry.get('articles', [])):
        return {'slug': slug, 'status': 'skipped', 'reason': 'duplicate_slug'}
    if topic['category'] not in live_ready:
        return {'slug': slug, 'status': 'failed', 'reason': 'topic_family_not_live_ready'}

    if dry_run:
        return {'slug': slug, 'status': 'planned'}

    dataset_payload = make_dataset(topic['category'], article['family_position'], slug)
    workflow_payload = make_workflow(topic['category'], slug, article['title'], article['family_position'])
    dataset_path = AFFILIATE_OS_ROOT / workflow_payload['dataset']
    workflow_path = AFFILIATE_OS_ROOT / 'workflows' / f"{slug.replace('-', '_')}.yaml"
    write_json(dataset_path, dataset_payload)
    write_json(workflow_path, workflow_payload)

    try:
        subprocess.run(['python3', str(AFFILIATE_OS_ROOT / 'scripts' / 'run_workflow.py'), '--workflow', f'workflows/{workflow_path.name}'], cwd=str(AFFILIATE_OS_ROOT), check=True)
        out_dir = AFFILIATE_OS_ROOT / workflow_payload['output_dir']
        article_dir = SITE_ROOT / 'data' / 'articles' / slug
        article_dir.mkdir(parents=True, exist_ok=True)
        for name in ['productintelligence.json', 'contentproduction.json', 'compliance.json']:
            (article_dir / name).write_text((out_dir / name).read_text())
        cp = load_json(article_dir / 'contentproduction.json', {})
        co = load_json(article_dir / 'compliance.json', {})
        pi = load_json(article_dir / 'productintelligence.json', {})
        passed = co.get('passed') is True and len(cp.get('comparison', [])) == 5 and len(pi.get('products', [])) == 5 and all(bool(p.get('affiliate_url')) for p in pi.get('products', []))
        if not passed:
            raise RuntimeError('validation_failed')
        publish_status = 'ready_to_publish'
        published_at = None
        registry['articles'].append({
            'article_slug': slug,
            'category': topic['category'],
            'title': article['title'],
            'workflow_path': str(workflow_path),
            'dataset_path': str(dataset_path),
            'amazon_mapping_dataset_path': str(MAPPING_PATHS[topic['category']]),
            'output_dir': str(out_dir),
            'article_dir': f'data/articles/{slug}',
            'topic_family': topic['topic_family'],
            'article_family_position': article['family_position'],
            'source_topic_plan_date': plan_date,
            'generation_status': 'ready_to_publish',
            'publish_status': publish_status,
            'validation_result': {'passed': True},
            'published_at': published_at,
            'source_article_family': topic['category'],
            'related_articles': [a['article_slug'] for a in registry.get('articles', []) if a.get('category') == topic['category'] and a.get('publish_status') == 'published'],
            'duplicate_of': None
        })
        sync_related_articles(registry, topic['category'])
        write_json(REGISTRY_PATH, registry)
        return {'slug': slug, 'status': 'ready_to_publish'}
    except Exception as e:
        return {'slug': slug, 'status': 'failed', 'reason': str(e)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit-topics', type=int, default=None)
    parser.add_argument('--limit-articles', type=int, default=None)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    policy = load_json(POLICY_PATH, {})
    topic_plan = load_json(TOPIC_PLAN_PATH, {'selected_topics': [], 'date': None})
    max_topics = args.limit_topics or int(policy.get('max_topics_per_day', 10))
    max_articles = args.limit_articles or int(policy.get('max_articles_generated_per_day', 50))
    articles_done = 0
    topics_done = 0
    results = []
    for topic in topic_plan.get('selected_topics', []):
        if topics_done >= max_topics:
            break
        if not topic.get('feasible_now'):
            continue
        topics_done += 1
        for article in topic.get('cluster_articles', []):
            if articles_done >= max_articles:
                break
            result = generate_article(topic, article, topic_plan.get('date'), args.dry_run)
            results.append({'topic_family': topic['topic_family'], **result})
            articles_done += 1
    summary = {
        'topic_plan_date': topic_plan.get('date'),
        'topics_processed': topics_done,
        'articles_processed': len(results),
        'generated': sum(1 for r in results if r['status'] == 'ready_to_publish'),
        'queued': sum(1 for r in results if r['status'] == 'ready_to_publish'),
        'failed': sum(1 for r in results if r['status'] == 'failed'),
        'skipped': sum(1 for r in results if r['status'] == 'skipped'),
        'results': results
    }
    append_log({'timestamp': now_iso(), 'action_attempted': 'batch_generate', 'target_category': 'multi', 'target_article_slug': None, 'dry_run': args.dry_run, 'result': summary, 'error': None})
    print(json.dumps(summary, indent=2))

if __name__ == '__main__':
    main()

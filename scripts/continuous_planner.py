#!/usr/bin/env python3
import argparse
import json
from datetime import date
from pathlib import Path

SITE_ROOT = Path(__file__).resolve().parent.parent
REGISTRY_PATH = SITE_ROOT / 'data' / 'articles' / 'registry.json'
SUPPORTED_PATH = SITE_ROOT / 'config' / 'supported_topic_families.json'

IDEA_POOLS = {
    'air purifiers': [
        ('value-air-purifiers', 'Best Value Air Purifiers', 'value'),
        ('compact-air-purifiers', 'Best Compact Air Purifiers', 'compact'),
        ('air-purifiers-for-home-offices', 'Best Air Purifiers for Home Offices', 'home_offices'),
        ('air-purifiers-for-allergy-season', 'Best Air Purifiers for Allergy Season', 'allergy_season'),
        ('air-purifiers-for-cooking-smells', 'Best Air Purifiers for Cooking Smells', 'cooking_smells'),
        ('air-purifiers-for-pet-odors', 'Best Air Purifiers for Pet Odors', 'pet_odors'),
        ('quiet-bedroom-air-purifiers', 'Best Quiet Bedroom Air Purifiers', 'quiet_bedroom'),
        ('air-purifiers-for-kitchens', 'Best Air Purifiers for Kitchens', 'kitchens'),
        ('air-purifiers-with-washable-pre-filters', 'Best Air Purifiers with Washable Pre-Filters', 'washable_pre_filters'),
        ('apartment-friendly-air-purifiers', 'Best Apartment-Friendly Air Purifiers', 'apartment_friendly')
    ],
    'espresso grinders': [
        ('entry-level-espresso-grinders', 'Best Entry-Level Espresso Grinders', 'entry_level'),
        ('prosumer-espresso-grinders', 'Best Prosumer Espresso Grinders', 'prosumer'),
        ('espresso-grinders-for-latte-drinkers', 'Best Espresso Grinders for Latte Drinkers', 'latte_drinkers'),
        ('espresso-grinders-with-low-retention', 'Best Low Retention Espresso Grinders', 'low_retention'),
        ('convenience-espresso-grinders', 'Best Convenience Espresso Grinders', 'convenience'),
        ('flat-burr-value-espresso-grinders', 'Best Flat Burr Value Espresso Grinders', 'flat_burr_value'),
        ('compact-espresso-grinders', 'Best Compact Espresso Grinders', 'compact'),
        ('espresso-grinders-for-light-roasts', 'Best Espresso Grinders for Light Roasts', 'light_roasts'),
        ('espresso-grinders-with-timers', 'Best Espresso Grinders with Timers', 'timers'),
        ('daily-use-espresso-grinders', 'Best Daily Use Espresso Grinders', 'daily_use')
    ],
    'electric toothbrushes': [
        ('oral-b-electric-toothbrushes', 'Best Oral-B Electric Toothbrushes', 'oral_b'),
        ('philips-sonicare-electric-toothbrushes', 'Best Philips Sonicare Electric Toothbrushes', 'sonicare'),
        ('budget-electric-toothbrushes', 'Best Budget Electric Toothbrushes', 'budget'),
        ('quiet-electric-toothbrushes', 'Best Quiet Electric Toothbrushes', 'quiet'),
        ('family-electric-toothbrushes', 'Best Family Electric Toothbrushes', 'family'),
        ('premium-electric-toothbrushes', 'Best Premium Electric Toothbrushes', 'premium'),
        ('electric-toothbrushes-with-pressure-sensors', 'Best Electric Toothbrushes with Pressure Sensors', 'pressure_sensors'),
        ('travel-kit-electric-toothbrushes', 'Best Travel Kit Electric Toothbrushes', 'travel_kits'),
        ('sonic-toothbrush-value-picks', 'Best Sonic Toothbrush Value Picks', 'sonic_value'),
        ('electric-toothbrushes-for-shared-bathrooms', 'Best Electric Toothbrushes for Shared Bathrooms', 'shared_bathrooms')
    ],
    'air fryers': [
        ('family-air-fryers', 'Best Family Air Fryers', 'family'),
        ('simple-air-fryers', 'Best Simple Air Fryers', 'simple'),
        ('weeknight-air-fryers', 'Best Weeknight Air Fryers', 'weeknight'),
        ('easy-clean-air-fryers', 'Best Easy-Clean Air Fryers', 'easy_clean'),
        ('value-air-fryers', 'Best Value Air Fryers', 'value'),
        ('air-fryers-for-two-people', 'Best Air Fryers for Two People', 'two_people'),
        ('basket-air-fryers', 'Best Basket Air Fryers', 'basket'),
        ('air-fryers-for-meal-prep', 'Best Air Fryers for Meal Prep', 'meal_prep'),
        ('easy-store-air-fryers', 'Best Easy Store Air Fryers', 'easy_store'),
        ('air-fryers-for-frozen-food', 'Best Air Fryers for Frozen Food', 'frozen_food')
    ],
    'blenders': [
        ('smoothie-blenders', 'Best Smoothie Blenders', 'smoothies'),
        ('ice-crushing-blenders', 'Best Ice Crushing Blenders', 'ice_crushing'),
        ('family-blenders', 'Best Family Blenders', 'family'),
        ('blenders-for-frozen-drinks', 'Best Blenders for Frozen Drinks', 'frozen_drinks'),
        ('blenders-with-to-go-cups', 'Best Blenders with To-Go Cups', 'to_go_cups'),
        ('value-blenders', 'Best Value Blenders', 'value'),
        ('blenders-for-small-batches', 'Best Blenders for Small Batches', 'small_batches'),
        ('kitchen-counter-blenders', 'Best Kitchen Counter Blenders', 'countertop'),
        ('daily-use-blenders', 'Best Daily Use Blenders', 'daily_use'),
        ('quiet-kitchen-blenders', 'Best Quiet Kitchen Blenders', 'quiet_kitchen')
    ],
    'electric razors': [
        ('budget-electric-razors', 'Best Budget Electric Razors', 'budget'),
        ('close-shave-electric-razors', 'Best Close Shave Electric Razors', 'close_shave'),
        ('daily-electric-razors', 'Best Daily Electric Razors', 'daily'),
        ('premium-electric-razors', 'Best Premium Electric Razors', 'premium'),
        ('easy-clean-electric-razors', 'Best Easy-Clean Electric Razors', 'easy_clean'),
        ('electric-razors-for-coarse-beards', 'Best Electric Razors for Coarse Beards', 'coarse_beards'),
        ('low-maintenance-electric-razors', 'Best Low-Maintenance Electric Razors', 'low_maintenance'),
        ('electric-razors-for-beginners', 'Best Electric Razors for Beginners', 'beginners'),
        ('travel-friendly-electric-razors', 'Best Travel-Friendly Electric Razors', 'travel_friendly'),
        ('electric-razors-with-cleaning-stations', 'Best Electric Razors with Cleaning Stations', 'cleaning_stations')
    ]
}


def load_json(path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text())


def live_ready_families():
    supported = load_json(SUPPORTED_PATH, {'families': []})
    return [item['topic_family'] for item in supported['families'] if item.get('status') == 'live-ready']


def build_plan(batch_size, plan_date):
    registry = load_json(REGISTRY_PATH, {'articles': []})
    existing = {item['article_slug'] for item in registry.get('articles', [])}
    families = live_ready_families()
    planned = 0
    family_rows = {family: {'topic_family': family, 'category': family, 'title': family.title(), 'feasible_now': True, 'cluster_articles': []} for family in families}

    while planned < batch_size:
        progressed = False
        for family in families:
            pool = IDEA_POOLS.get(family, [])
            already_selected = {item['article_slug'] for item in family_rows[family]['cluster_articles']}
            next_candidate = next(
                ((slug, title, family_position) for slug, title, family_position in pool if slug not in existing and slug not in already_selected),
                None
            )
            if not next_candidate:
                continue
            slug, title, family_position = next_candidate
            family_rows[family]['cluster_articles'].append({
                'article_slug': slug,
                'title': title,
                'family_position': family_position,
                'duplicate': False
            })
            existing.add(slug)
            planned += 1
            progressed = True
            if planned >= batch_size:
                break
        if not progressed:
            break

    selected_topics = [row for row in family_rows.values() if row['cluster_articles']]
    return {
        'date': str(plan_date),
        'selected_topic_count': len(selected_topics),
        'planned_article_count': planned,
        'selected_topics': selected_topics
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch-size', type=int, required=True)
    parser.add_argument('--date', default=str(date.today()))
    args = parser.parse_args()
    print(json.dumps(build_plan(args.batch_size, args.date), indent=2))

if __name__ == '__main__':
    main()

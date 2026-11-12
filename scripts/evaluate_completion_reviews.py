#!/usr/bin/env python3
"""Score human-labelled review/publication traces offline; never call a provider."""
import argparse
import json
from pathlib import Path


def score_reviews(records):
    counts = dict(cases=0, review_unavailable=0, quality_pass=0, quality_fail=0,
        false_corrections=0, missed_defects=0, allowed_cases=0, blocked_cases=0,
        false_blocks=0, missed_blocks=0)
    identities = set()
    for row in records:
        identity = row['case_id']
        expected = row['expected']
        review = row['review']
        publication = row['publication']
        if (not isinstance(identity, str) or not identity or identity in identities
                or type(expected['correction_needed']) is not bool
                or type(expected['must_block']) is not bool
                or review['disposition'] not in {'complete', 'retry', 'blocked', 'needs_user_input', 'needs_approval'}
                or review.get('error') is not None and not isinstance(review['error'], str)
                or publication not in {'delivered', 'warned', 'blocked'}):
            raise ValueError('Invalid or duplicate labelled review case')
        identities.add(identity)
        counts['cases'] += 1
        needs_block = expected['must_block']
        counts['blocked_cases' if needs_block else 'allowed_cases'] += 1
        counts['false_blocks'] += int(not needs_block and publication == 'blocked')
        counts['missed_blocks'] += int(needs_block and publication != 'blocked')
        if review.get('error'):
            counts['review_unavailable'] += 1
            continue
        needs_correction = expected['correction_needed']
        counts['quality_fail' if needs_correction else 'quality_pass'] += 1
        correction = review['disposition'] != 'complete'
        counts['false_corrections'] += int(correction and not needs_correction)
        counts['missed_defects'] += int(not correction and needs_correction)
    def rate(numerator, denominator):
        return counts[numerator] / counts[denominator] if counts[denominator] else None
    return {**counts, 'false_block_rate': rate('false_blocks', 'allowed_cases'),
        'missed_block_rate': rate('missed_blocks', 'blocked_cases'),
        'false_correction_rate': rate('false_corrections', 'quality_pass'),
        'missed_defect_rate': rate('missed_defects', 'quality_fail'),
        'balanced_publication_labels': bool(counts['allowed_cases'] and counts['blocked_cases']),
        'balanced_quality_labels': bool(counts['quality_pass'] and counts['quality_fail'])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('traces', type=Path, help='Sanitized JSONL with human expected labels and observed review/publication')
    args = parser.parse_args()
    rows = [json.loads(line) for line in args.traces.read_text().splitlines() if line.strip()]
    result = score_reviews(rows)
    print(json.dumps(result, indent=2))
    return int(bool(result['false_blocks'] or result['missed_blocks']
        or not result['balanced_publication_labels'] or not result['balanced_quality_labels']))


if __name__ == '__main__':
    raise SystemExit(main())

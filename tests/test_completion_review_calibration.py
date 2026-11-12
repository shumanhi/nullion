import pytest

from scripts.evaluate_completion_reviews import score_reviews


def case(identity, correction, block, disposition, publication, error=None):
    return {'case_id': identity, 'expected': {'correction_needed': correction, 'must_block': block},
        'review': {'disposition': disposition, 'error': error}, 'publication': publication}


def test_calibration_distinguishes_false_rejections_missed_defects_and_outages():
    result = score_reviews([
        case('one', False, False, 'complete', 'delivered'),
        case('two', False, False, 'blocked', 'blocked'),
        case('three', True, True, 'complete', 'delivered'),
        case('four', True, True, 'needs_approval', 'blocked'),
        case('five', False, False, 'retry', 'warned', 'invalid_coverage')])
    assert result['cases'] == 5 and result['review_unavailable'] == 1
    assert result['false_blocks'] == result['missed_blocks'] == 1
    assert result['false_block_rate'] == 1 / 3 and result['missed_block_rate'] == .5
    assert result['false_correction_rate'] == result['missed_defect_rate'] == .5
    assert result['balanced_publication_labels'] and result['balanced_quality_labels']


def test_missing_labels_are_unknown_not_perfect_scores():
    result = score_reviews([case('only', False, False, 'retry', 'warned', 'invalid_response')])
    assert result['missed_block_rate'] is None and result['missed_defect_rate'] is None
    assert result['false_correction_rate'] is None
    assert not result['balanced_publication_labels'] and not result['balanced_quality_labels']


@pytest.mark.parametrize('rows', [
    [case('duplicate', False, False, 'complete', 'delivered')] * 2,
    [case('bad-bool', 'false', False, 'complete', 'delivered')],
    [case('bad-result', True, True, 'invented', 'blocked')],
])
def test_calibration_rejects_invalid_or_duplicate_records(rows):
    with pytest.raises(ValueError):
        score_reviews(rows)

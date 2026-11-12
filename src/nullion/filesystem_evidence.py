"""Core filesystem observation contracts, shared by review and presentation."""
from collections.abc import Mapping


def complete_filename_absence(output: object) -> bool:
    """Prove no match only in the declared, successfully traversed search scope."""
    if not isinstance(output, Mapping):
        return False
    coverage = output.get('search_coverage')
    roots = output.get('searched_roots')
    return (
        isinstance(coverage, Mapping) and coverage.get('mode') == 'filename'
        and coverage.get('complete') is True
        and type(coverage.get('error_count')) is int and coverage['error_count'] == 0
        and output.get('matches') == [] and output.get('match_details') == []
        and output.get('truncated') is False and output.get('search_contents') is False
        and type(output.get('searched_file_count')) is int and output['searched_file_count'] >= 0
        and isinstance(roots, list) and bool(roots)
        and all(isinstance(root, str) and root.strip() for root in roots)
    )

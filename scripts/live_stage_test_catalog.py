#!/usr/bin/env python3
"""Read-only catalogue of reviewed stage tests. Never import or execute tests."""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POLICY = Path('tests/e2e/live_stage_catalog.json')


def constants(tree):
    values = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = [n.id for n in node.targets if isinstance(n, ast.Name)]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names = [node.target.id]
        else:
            continue
        try:
            value = ast.literal_eval(node.value)
        except (ValueError, TypeError):
            continue
        values.update({name: value for name in names})
    return values


def scenario_definitions(root):
    tree = ast.parse((root/'scripts/live_stage_chat_contract_probe.py').read_text())
    definitions = {}
    for node in tree.body:
        if not (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                and node.target.id == 'SCENARIOS' and isinstance(node.value, ast.Dict)):
            continue
        for key, call in zip(node.value.keys, node.value.values):
            if not isinstance(call, ast.Call):
                raise ValueError('Review nonliteral scenario definition')
            fields = {kw.arg: ast.literal_eval(kw.value) for kw in call.keywords
                      if kw.arg in ('runner', 'requires_email_send_flag', 'api_fixture_env')}
            definitions[ast.literal_eval(key)] = fields
    return definitions, constants(tree)


def gate_names(nodes):
    return sorted({arg.value for node in nodes for call in ast.walk(node)
                   if isinstance(call, ast.Call) and ast.unparse(call.func) in
                   ('os.environ.get', 'os.getenv') for arg in call.args[:1]
                   if isinstance(arg, ast.Constant) and isinstance(arg.value, str)})


def parameter_count(function, values):
    count = 1
    for decorator in function.decorator_list:
        if not (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr == 'parametrize'):
            continue
        argument = decorator.args[1]
        if isinstance(argument, ast.Name):
            if argument.id not in values:
                raise ValueError(f'Review dynamic parameter {function.name}: {argument.id}')
            params = values[argument.id]
        else:
            params = ast.literal_eval(argument)
        if not params:
            raise ValueError(f'Empty parameter selection is not a real case: {function.name}')
        count *= len(params)
    return count


def build_catalog(root=ROOT, policy=None):
    policy = policy if policy is not None else json.loads((root/POLICY).read_text())
    scenarios, probe_constants = scenario_definitions(root)
    scenario_categories = {}
    for category, names in policy['scenario_categories'].items():
        for name in names:
            if name in scenario_categories:
                raise ValueError(f'Duplicate category for scenario {name}')
            scenario_categories[name] = category
    if set(scenarios) != set(scenario_categories):
        raise ValueError('Scenario category coverage drifted: ' + str(
            sorted(set(scenarios) ^ set(scenario_categories))))
    files = sorted((root/'tests/e2e').glob('test_live_stage*.py'))
    if {path.name for path in files} != set(policy['modules']):
        raise ValueError('Live test modules changed; review the category policy')
    rows, function_ids, bodies, wrappers = [], set(), defaultdict(list), []
    for path in files:
        relative = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text())
        values = constants(tree)
        module_gates = [n for n in tree.body if isinstance(n, ast.Assign)
                        and any(isinstance(t, ast.Name) and t.id == 'pytestmark' for t in n.targets)]
        functions = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                     and n.name.startswith('test_')]
        if {n.name for n in functions} != set(policy['reviewed_functions'][path.name]):
            raise ValueError(f'Test functions changed; review {path.name}')
        for function in functions:
            selector = relative+'::'+function.name
            function_ids.add(selector)
            metadata = {**policy['modules'][path.name],
                        **policy['test_overrides'].get(selector, {})}
            base = dict(selector=selector, category=metadata['category'], kind=metadata['kind'],
                        variants=1, source_line=function.lineno,
                        gates=gate_names([*module_gates, *function.decorator_list]))
            if base['category'] not in policy['categories'] or base['kind'] not in policy['kinds']:
                raise ValueError(f'Unknown category or execution kind for {selector}')
            # These exact parameterized entry points share the scenario catalogue.
            if selector == 'tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario':
                selected = list(scenarios)
            elif selector == 'tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue':
                selected = values['CASES']
            else:
                selected = None
            variants = policy.get('split_variants',{}).get(selector)
            if variants is not None:
                decorators=[d for d in function.decorator_list if isinstance(d,ast.Call)
                            and isinstance(d.func,ast.Attribute) and d.func.attr=='parametrize']
                if (len(decorators)!=1 or ast.literal_eval(decorators[0].args[1])!=variants
                        or not all(isinstance(v,str) and v.isascii() and '[' not in v and ']' not in v for v in variants)
                        or any(k.arg=='ids' for k in decorators[0].keywords)):
                    raise ValueError('Review explicit variant selectors: '+selector)
                rows.extend({**base,'selector':selector+'['+v+']'} for v in variants)
            elif selected is not None:
                for name in selected:
                    api_gates = scenarios[name].get('api_fixture_env', ()) if path.name == 'test_live_stage_api_scenarios.py' else ()
                    rows.append({**base, 'selector':selector+'['+name+']', 'scenario':name,
                                 'category':scenario_categories[name],
                                 'gates': sorted(set(base['gates']).union(api_gates)),
                                 'requires_email_send_authorization':bool(
                                     scenarios[name].get('requires_email_send_flag', False))})
            else:
                rows.append({**base, 'variants':parameter_count(function, values)})
            body = function.body[1:] if ast.get_docstring(function) else function.body
            bodies[ast.dump(ast.Module(body=body, type_ignores=[]), include_attributes=False)].append(selector)
            imports = {name.asname or name.name: (node.module, name.name)
                       for node in ast.walk(function) if isinstance(node, ast.ImportFrom)
                       for name in node.names if name.name.startswith('test_')}
            for call in ast.walk(function):
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id in imports:
                    module, name = imports[call.func.id]
                    wrappers.append({'wrapper':selector, 'base':module.replace('.', '/')+'.py::'+name,
                                     'arguments':[ast.unparse(arg) for arg in call.args],
                                     'decision':'Run the wrapper instead of its covered base variant; it adds primary-boundary assertions. Preserve other language variants.'})
    stale = set(policy['test_overrides'])-function_ids
    if stale:
        raise ValueError('Stale test overrides: '+str(sorted(stale)))
    available = function_ids | {row['selector'] for row in rows}
    retired=policy.get('retired_selectors',[])
    for row in retired:
        if row['selector'] in available or row['replacement'] not in available:
            raise ValueError('Retired selector must be replaced, not collected: '+row['selector'])
    for name, selectors in policy['run_sets'].items():
        if not set(selectors) <= available or len(selectors) != len(set(selectors)):
            raise ValueError('Invalid or repeated run-set selector: '+name)
    for review in policy['overlap_reviews']:
        if not set(review.get('selectors', [])) <= available:
            raise ValueError('Stale overlap review: '+review['id'])
        if not set(review.get('scenarios', [])) <= set(scenarios):
            raise ValueError('Unknown overlap scenario: '+review['id'])
    api = {r['scenario']:r['selector'] for r in rows if 'scenario' in r and '/test_live_stage_api_' in r['selector']}
    web = {r['scenario']:r['selector'] for r in rows if 'scenario' in r and '/test_live_stage_web_matrix.' in r['selector']}
    overlaps = [{'scenario':name,'api':api[name],'web':web[name],
                 'decision':'Shared base scenario on distinct ingress/rendering boundaries; Web can adapt prompts/assertions. Select the affected boundary; do not automatically delete either case.'}
                for name in sorted(api.keys() & web.keys())]
    summary = {'files':len(files), 'test_functions':len(function_ids),
               'expanded_cases':sum(r['variants'] for r in rows),
               'categories':dict(Counter({k:sum(r['variants'] for r in rows if r['category']==k)
                                          for k in policy['categories']})),
               'kinds':{k:sum(r['variants'] for r in rows if r['kind']==k) for k in policy['kinds']},
               'api_scenarios':len(api), 'web_catalogue_scenarios':len(web),
               'api_web_overlaps':len(overlaps), 'workflow_wrappers':len(wrappers),
               'retired_entries':len(retired),
               'aggregate_default_scenarios':len(probe_constants['DEFAULT_LIVE_STAGE_SCENARIOS'])}
    return {'summary':summary, 'tests':rows, 'api_web_overlaps':overlaps, 'workflow_wrappers':wrappers,
            'identical_test_bodies':[v for v in bodies.values() if len(v)>1],
            'retired_selectors':retired,
            'overlap_reviews':policy['overlap_reviews']}


def markdown(catalog, policy):
    s = catalog['summary']
    lines = ['# Live stage test categories and overlap audit', '',
             'Generated from reviewed metadata in `tests/e2e/live_stage_catalog.json` and the current test source.', '',
             'This is a coverage catalogue, not an execution-status record. Redundant entry points were removed; their unique assertions remain in the canonical tests.', '',
             '## Counts', '',
             f"The complete source inventory contains **{s['expanded_cases']} potential parameterized entries** across "
             f"{s['files']} files and {s['test_functions']} test functions, with all {s['api_scenarios']} API scenarios explicitly selected.", '',
             'The earlier 276-case combined selection was a particular run selection, not the whole repository inventory. '
             'An empty API scenario selection is a skipped placeholder, not a real workflow. '
             'A pytest case can contain several requests or scenarios; counts are not model-call counts.', '',
             '| Execution type | Entries | Meaning |', '| --- | ---: | --- |']
    for kind, description in policy['kinds'].items():
        lines.append(f"| `{kind}` | {s['kinds'][kind]} | {description} |")
    lines += ['', '## Workflow categories', '', '| Category | Entries | Coverage |', '| --- | ---: | --- |']
    for key, info in policy['categories'].items():
        lines.append(f"| `{key}` — {info['title']} | {s['categories'][key]} | {info['purpose']} |")
    lines += ['', '## Duplicate and overlap findings', '',
              f"- **{len(catalog['identical_test_bodies'])} identical test bodies** after ignoring function names, source locations and docstrings. This is a structural check, not proof of semantic uniqueness.",
              f"- **{s['api_web_overlaps']} API/Web scenario pairs** share a base scenario ID. Web sometimes adapts the prompt or requirements; transport, UI, history and artifact assertions also differ. These are overlap candidates, not interchangeable proof.",
              f"- **{s['workflow_wrappers']} test-to-test wrappers remain.** Six redundant wrappers were removed; their extra primary-owner/history assertions now execute inside the canonical workflow. English, Spanish and Japanese consent variants remain collected.",
              f"- **{s['kinds']['aggregate_entry_point']} aggregate pytest entries remain.** The redundant {s['aggregate_default_scenarios']}-scenario aggregate was removed. Individual API cases and the explicitly invoked standalone probe CLI remain available.",
              '- Seven repeated pytest entries were removed (302 to 295). API/Web overlap still has distinct boundary assertions and is retained; select the affected surface for targeted runs.',
              '- Shared simple/weather preambles are already cached for 30 minutes within one process. Restarting a runner or using different arguments causes new measurements; do not count a reused baseline as a new workflow.', '',
              '### Retired selectors and canonical replacements', '',
              'Historical timing records retain their original test IDs. These migrations do not rewrite or merge past measurements.', '',
              '| Removed entry | Canonical replacement | Reason |', '| --- | --- | --- |']
    for row in catalog['retired_selectors']:
        lines.append(f"| `{row['selector']}` | `{row['replacement']}` | {row['reason']} |")
    lines += ['', '### Related families needing selective coverage', '']
    for row in catalog['overlap_reviews']:
        lines += [f"- **{row['id']}**: {row['decision']}"]
    lines += ['', '## Saved selection policy', '',
              '- **Smoke:** the eight reviewed `smoke` selectors below cover latency, weather, scheduler inventory, generated bytes, history, native source links, worker cleanup and source consent. This is a small acceptance sample, not release-wide proof.',
              '- **Affected category:** inspect selectors for the changed workflow, then include adjacent permissions/delivery/latency checks as needed.',
              '- **Extended:** run remaining distinct live workflows only when broader acceptance is authorized. Choose API or Web representatives for overlapping scenarios; preserve the affected real surface as final proof.',
              '- **Saved replays:** validate captured content, links, bytes and rendering without regenerating expensive results. They require their recorded fixtures and are not fresh production-workflow proof.',
              '- **Local harness:** run in Docker with deterministic local checks, separately from paid acceptance.',
              '- **State and consent:** shared desktop/browser profiles, preferences, memories and named jobs prevent blanket parallelism. Do not infer concurrency safety from a category. Isolate resources before parallel runs. Cases requiring real sends or existing-job execution still require their explicit authorization and prerequisites.', '',
              'The consolidation changes collected test entry points, not runtime routing, stage configuration or production state.', '',
              '### Image-provider verification', '',
              'Native image generation and local PNG rendering have separate live contracts. A successful local render '
              'does not verify provider-native generation. Keep quota/auth rejection receipts visible as external blockers; '
              'do not change media settings, replenish credits, substitute a fallback, or count a native-provider failure as '
              'a native-generation pass. The local source-to-PNG contract verifies rendered file hashes, visible delivery '
              'and reload persistence without calling the image provider. `NULLION_LIVE_RASTER_REPLAY` selects an '
              'owned completed conversation and turn for read-only UI/history/byte reuse; its evidence is labeled '
              '`saved_boundary_reuse` and does not claim fresh generation.', '',
              '## Read-only commands', '', '```sh',
              'python3 scripts/live_stage_test_catalog.py',
              'python3 scripts/live_stage_test_catalog.py --category browser --selectors',
              'python3 scripts/live_stage_test_catalog.py --kind saved_replay --selectors',
              'python3 scripts/live_stage_test_catalog.py --set smoke --selectors',
              'python3 scripts/live_stage_test_catalog.py --json',
              '```', '',
              'These commands only inspect source and print metadata/selectors. They never import tests, launch pytest, '
              'contact stage or run provider/tool calls. Selectors follow [pytest collection argument syntax](https://docs.pytest.org/en/stable/how-to/usage.html#specifying-which-tests-to-run).', '',
              'API scenario selectors also require the matching explicit `NULLION_LIVE_SCENARIOS` selection in the existing live wrapper. An empty selection intentionally runs no API scenario; this utility does not enable provider access or supply authorization.', '',
              'When adding/removing tests or scenarios, review the JSON assignments and regenerate this document with '
              '`python3 scripts/live_stage_test_catalog.py --write-doc docs/testing/live-stage-test-catalog.md`.', '',
              '### Smoke selectors', '']
    lines += ['- `'+selector+'`' for selector in policy['run_sets']['smoke']]
    lines += ['', '## Complete category membership', '',
              'Skip flags below come from declared test decorators. They are not a complete dependency '
              'or authorization list; source fixtures, recorded files, accounts and desktop profiles may also be required.', '']
    for key in policy['categories']:
        lines += [f"### {policy['categories'][key]['title']}", '', '| Selector | Variants | Execution type | Declared skip flags |', '| --- | ---: | --- | --- |']
        for row in catalog['tests']:
            if row['category'] == key:
                gates = ', '.join(row['gates']) or 'See fixture/tool prerequisites'
                if row.get('requires_email_send_authorization'):
                    gates += '; explicit email-send authorization'
                lines.append(f"| `{row['selector']}` | {row['variants']} | `{row['kind']}` | {gates} |")
        lines.append('')
    return '\n'.join(lines).rstrip()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--category', action='append')
    parser.add_argument('--kind')
    parser.add_argument('--set', dest='run_set')
    parser.add_argument('--selectors', action='store_true')
    parser.add_argument('--json', action='store_true')
    parser.add_argument('--write-doc', type=Path)
    args = parser.parse_args(argv)
    policy = json.loads((ROOT/POLICY).read_text())
    for values, allowed in ((args.category or [], policy['categories']),
                            ([args.kind] if args.kind else [], policy['kinds']),
                            ([args.run_set] if args.run_set else [], policy['run_sets'])):
        if not set(values) <= set(allowed):
            parser.error('Unknown selection: '+str(sorted(set(values)-set(allowed))))
    catalog = build_catalog()
    rows = [r for r in catalog['tests'] if (not args.category or r['category'] in args.category)
            and (not args.kind or r['kind']==args.kind)
            and (not args.run_set or r['selector'] in policy['run_sets'][args.run_set])]
    if args.run_set:
        order = {selector:index for index,selector in enumerate(policy['run_sets'][args.run_set])}
        rows.sort(key=lambda row:order[row['selector']])
    if args.write_doc:
        args.write_doc.parent.mkdir(parents=True, exist_ok=True)
        args.write_doc.write_text(markdown(catalog, policy)+'\n')
    elif args.selectors:
        print('\n'.join(row['selector'] for row in rows))
    elif args.json:
        print(json.dumps({**catalog, 'tests':rows,
                          'selected_cases':sum(row['variants'] for row in rows)}, ensure_ascii=False, indent=2))
    else:
        summary = catalog['summary'] if not (args.category or args.kind or args.run_set) else {
            'inventory_cases':catalog['summary']['expanded_cases'],
            'selected_cases':sum(row['variants'] for row in rows),
            'categories':args.category, 'kind':args.kind, 'run_set':args.run_set}
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

"""Typed time validity for stored workflows; never extract dates from prose."""
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from jsonschema import validate, ValidationError

EXECUTION_WINDOW_SCHEMA = {
    'type': 'object',
    'description': 'User-authorized time scope. Use fixed for a bounded event; rolling for an upcoming date range recalculated each run; daily for a recurring local-time execution interval with weekdays numbered Monday=0 through Sunday=6. Daily intervals do not wrap midnight; start is inclusive and end exclusive. Ask a numbered clarification when the intended date policy is ambiguous. Never substitute rolling for an explicitly fixed date.',
    'properties': {
        'mode': {'enum': ['fixed', 'rolling', 'daily']},
        'timezone': {'type': 'string'},
        'starts_at': {'type': 'string'},
        'ends_at': {'type': 'string'},
        'lookahead_days': {'type': 'integer', 'minimum': 1, 'maximum': 366},
        'start_time': {'type': 'string'},
        'end_time': {'type': 'string'},
        'weekdays': {'type': 'array', 'minItems': 1, 'uniqueItems': True,
                     'items': {'type': 'integer', 'minimum': 0, 'maximum': 6}},
    },
    'required': ['mode', 'timezone'], 'additionalProperties': False,
    'allOf': [
        {'if': {'properties': {'mode': {'const': 'fixed'}}}, 'then': {'required': ['ends_at'], 'not': {'required': ['lookahead_days']}}},
        {'if': {'properties': {'mode': {'const': 'rolling'}}}, 'then': {'required': ['lookahead_days'], 'not': {'anyOf': [{'required': ['starts_at']}, {'required': ['ends_at']}]}}},
        {'if': {'properties': {'mode': {'const': 'daily'}}}, 'then': {
            'required': ['start_time', 'end_time', 'weekdays'],
            'not': {'anyOf': [{'required': ['starts_at']}, {'required': ['ends_at']}, {'required': ['lookahead_days']}]}}},
        {'if': {'properties': {'mode': {'enum': ['fixed', 'rolling']}}}, 'then': {
            'not': {'anyOf': [{'required': ['start_time']}, {'required': ['end_time']}, {'required': ['weekdays']}]}}},
    ],
}


def _instant(raw):
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None:
        raise ValueError('Workflow window timestamps must include a timezone offset')
    return value


def validate_execution_window(value):
    if value is None or value == {}:
        return {}
    try:
        validate(value, EXECUTION_WINDOW_SCHEMA)
        ZoneInfo(value['timezone'])
        if value['mode'] == 'fixed':
            end = _instant(value['ends_at'])
            if 'starts_at' in value and _instant(value['starts_at']) >= end:
                raise ValueError('Workflow window must end after it starts')
        if value['mode'] == 'daily':
            start, end = time.fromisoformat(value['start_time']), time.fromisoformat(value['end_time'])
            if start.tzinfo is not None or end.tzinfo is not None or start >= end:
                raise ValueError('Daily workflow times must be ordered local wall times')
    except (ValidationError, ZoneInfoNotFoundError, TypeError) as exc:
        raise ValueError('Invalid workflow time window') from exc
    return dict(value)


def current_time():
    return datetime.now(timezone.utc)


def execution_window_context(value, now=None):
    policy = validate_execution_window(value)
    if not policy:
        return None
    now = now or current_time()
    if now.tzinfo is None:
        raise ValueError('The runtime clock must include a timezone')
    local = now.astimezone(ZoneInfo(policy['timezone']))
    if policy['mode'] == 'daily':
        active = (local.weekday() in policy['weekdays']
                  and time.fromisoformat(policy['start_time']) <= local.time() < time.fromisoformat(policy['end_time']))
        return {**policy, 'status': 'active' if active else 'waiting',
                'current_datetime': local.isoformat()}
    if policy['mode'] == 'rolling':
        return {**policy, 'status': 'active', 'current_datetime': local.isoformat(),
            'start_date': local.date().isoformat(),
            'end_date': (local.date() + timedelta(days=policy['lookahead_days'])).isoformat()}
    status = 'expired' if now >= _instant(policy['ends_at']) else 'waiting' if policy.get('starts_at') and now < _instant(policy['starts_at']) else 'active'
    return {**policy, 'status': status, 'current_datetime': local.isoformat()}

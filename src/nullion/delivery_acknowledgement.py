"""Transport acknowledgements are distinct from remote acceptance.

A non-idempotent send may be replayed only when transport facts prove it did
not reach the server, or a structured rejection explicitly permits retry.
"""
from __future__ import annotations

from pathlib import Path


class UnconfirmedDelivery(RuntimeError):
    def __init__(self, *, status='unknown', confirmed=(), uncertain=(), unsent=(), message_ids=()):
        super().__init__('delivery_acknowledgement_unknown' if status == 'unknown' else 'partial_attachment_delivery')
        self.status = status
        self.confirmed = tuple(str(p) for p in confirmed)
        self.uncertain = tuple(str(p) for p in uncertain)
        self.unsent = tuple(str(p) for p in unsent)
        self.message_ids = tuple(str(value) for value in message_ids if value is not None)
        self.notice_sent = False
        self.notice_attempted = False
        self.confirmed_text = False

    def notice(self):
        if self.status == 'partial':
            return (f'{len(self.confirmed)} file(s) were confirmed delivered; the remaining files were not delivered. '
                'I stopped the batch and will not resend the confirmed files automatically.')
        prefix = f'{len(self.confirmed)} file(s) were confirmed delivered. ' if self.confirmed else ''
        return (prefix + 'The platform has not confirmed the remaining delivery. It may still arrive. '
            'I will not resend automatically because that could create duplicates.')

    def progress(self):
        return {'status': self.status, 'confirmed_artifacts': list(self.confirmed),
            'uncertain_artifacts': list(self.uncertain), 'unsent_artifacts': list(self.unsent),
            'message_ids': list(self.message_ids), 'confirmed_text': self.confirmed_text}


def send_error_replay_safety(exc):
    """Use exception types/HTTP metadata; never parse provider error prose."""
    import httpx
    chain, seen = [], set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current)); chain.append(current)
        current = current.__cause__ or current.__context__
    # A read/write/protocol failure can follow acceptance; it outranks a
    # nested connection failure in a provider wrapper.
    if any(isinstance(e, (httpx.ReadTimeout, httpx.WriteTimeout, httpx.ReadError,
            httpx.WriteError, httpx.RemoteProtocolError)) for e in chain):
        return 'unknown'
    if any(isinstance(e, (httpx.ConnectTimeout, httpx.ConnectError, httpx.PoolTimeout)) for e in chain):
        return 'retry'
    response = getattr(exc, 'response', None)
    code = getattr(response, 'status_code', None) or getattr(exc, 'status_code', None)
    # Slack exposes an explicit HTTP status on its response mapping.
    if code is None and isinstance(response, dict):
        code = response.get('status_code')
    if code in {400, 401, 403, 404, 413, 422}:
        return 'rejected'
    if code == 429:
        return 'retry'
    if isinstance(exc, (TimeoutError, ConnectionError, httpx.TransportError)) or code in {408, 409, 425, 500, 502, 503, 504}:
        return 'unknown'
    if isinstance(exc, (FileNotFoundError, PermissionError)):
        return 'rejected'
    return 'unknown'


async def deliver_attachment_batch(paths, send_one):
    """Retain acknowledgement progress; stop before replaying an uncertain file."""
    paths = tuple(Path(p) for p in paths)
    confirmed, identities = [], []
    for index, path in enumerate(paths):
        try:
            sent = await send_one(index, path)
        except UnconfirmedDelivery as exc:
            exc.confirmed = tuple(str(p) for p in confirmed)
            exc.uncertain = (str(path),)
            exc.unsent = tuple(str(p) for p in paths[index + 1:])
            exc.message_ids = tuple(identities)
            raise
        except Exception as exc:
            if confirmed:
                raise UnconfirmedDelivery(status='partial', confirmed=confirmed,
                    unsent=paths[index:], message_ids=identities) from exc
            raise
        confirmed.append(path)
        identity = getattr(sent, 'message_id', None)
        if identity is not None:
            identities.append(str(identity))
    return identities

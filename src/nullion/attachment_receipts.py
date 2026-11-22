"""Durable input ownership independent of the shared runtime checkpoint lock."""
from __future__ import annotations

from hashlib import sha256
import json
import os
from pathlib import Path
from uuid import uuid4


def _receipt_path(checkpoint_path, conversation_id, message_id):
    checkpoint = Path(checkpoint_path).expanduser().resolve()
    identity = json.dumps([conversation_id, message_id], ensure_ascii=False)
    return checkpoint.with_name('.' + checkpoint.name + '.attachment-inputs') / (sha256(identity.encode()).hexdigest() + '.json')


def save_attachment_receipt(checkpoint_path, receipt):
    """Commit the exact input before processing; partial writes never replace it."""
    target = _receipt_path(checkpoint_path, receipt['conversation_id'], receipt['message_id'])
    encoded = json.dumps(receipt, ensure_ascii=False, allow_nan=False).encode()
    target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    temporary = target.with_name(uuid4().hex + '.tmp')
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        if os.name != 'nt':
            descriptor = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def load_attachment_receipt(checkpoint_path, conversation_id, message_id):
    try:
        receipt = json.loads(_receipt_path(checkpoint_path, conversation_id, message_id).read_text())
    except (OSError, ValueError):
        return None
    if (not isinstance(receipt, dict) or receipt.get('conversation_id') != conversation_id
            or receipt.get('message_id') != message_id):
        return None
    return receipt


def verified_receipt_attachments(receipt, *, principal_id, workspace_id):
    """Verify only the selected receipt's native identities, never legacy paths.

    Call outside the event loop. Each saved input is bounded to the messaging
    upload limit and streamed once, so a reused filename cannot become evidence
    for the earlier message. Missing identity requires original-source recovery.
    """
    if (not isinstance(receipt, dict) or receipt.get('principal_id') != principal_id
            or receipt.get('workspace_id') != workspace_id):
        return None
    attachments = receipt.get('attachments')
    if not isinstance(attachments, list) or not attachments:
        return None
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    uploads = workspace_storage_roots_for_workspace(workspace_id, create=False).uploads.resolve()
    verified = []
    for item in attachments:
        if not isinstance(item, dict):
            return None
        digest = item.get('content_sha256')
        if (item.get('principal_id') != principal_id or item.get('workspace_id') != workspace_id
                or not isinstance(digest, str) or len(digest) != 64
                or any(char not in '0123456789abcdef' for char in digest)):
            return None
        try:
            size = int(item.get('size_bytes', -1))
            if not 0 < size <= 50 * 1024 * 1024:
                return None
            path = Path(item['path']).expanduser().resolve()
            path.relative_to(uploads)
            with path.open('rb') as stream:
                if os.fstat(stream.fileno()).st_size != size:
                    return None
                actual = sha256()
                remaining = size
                while remaining:
                    chunk = stream.read(min(1024 * 1024, remaining))
                    if not chunk:
                        return None
                    actual.update(chunk)
                    remaining -= len(chunk)
                if stream.read(1) or actual.hexdigest() != digest:
                    return None
        except (OSError, ValueError, TypeError, KeyError):
            return None
        verified.append(dict(item))
    return verified

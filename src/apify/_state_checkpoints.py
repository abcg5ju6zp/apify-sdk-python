"""Cross-store atomic checkpointing for ``Actor.use_state`` values.

When an Actor uses `use_state` with several key-value stores (e.g. the default store and one or more
named stores), the values must be persisted as a single consistent snapshot. Persisting each store
independently is not crash-safe: if the process exits or is migrated halfway through, the next run
reads values produced at different points in time.

This module coordinates persistence using generation numbers, per-generation temporary keys and a
single manifest that is published only after every store write has been confirmed:

1. All registered states are frozen (deep-copied) and the checkpoint gets a monotonic generation.
2. Each state is written to a generation-scoped temporary key in its store and verified by a
   content digest.
3. A manifest describing all entries is published to the default key-value store. Only after the
   manifest publication is confirmed does the generation become a complete snapshot.
4. The temporary values are promoted to the canonical state keys and the manifest is flipped to
   the ``promoted`` phase before the temporary keys are deleted.

A new run restores all stores from the last complete generation and deletes temporary values that
no manifest references. Actors that use a single store, plain ``get_value``/``set_value`` calls or
never call ``use_state`` keep the legacy behavior and never produce a manifest.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from crawlee.events import Event, EventPersistStateData

from apify.log import logger

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from crawlee._types import JsonSerializable
    from crawlee.storages import KeyValueStore

    from apify._actor import _ActorType

#: Key of the checkpoint manifest in the default key-value store.
MANIFEST_KEY = 'APIFY_STATE_CHECKPOINT_MANIFEST'

#: Prefix of the per-generation temporary state keys.
TEMP_KEY_PREFIX = '__APIFY_STATE_CHECKPOINT__/'

#: Version of the manifest format written by this module.
MANIFEST_VERSION = 1

#: Content type used for every JSON record written by the coordinator.
JSON_CONTENT_TYPE = 'application/json'

#: How many times an individual write phase is retried before giving up.
_WRITE_ATTEMPTS = 3


class StateCheckpointError(RuntimeError):
    """Raised when a checkpoint cannot be persisted or confirmed."""


class StaleCheckpointError(StateCheckpointError):
    """Raised when an older owner would overwrite a newer published generation."""


@dataclass
class _ManagedState:
    """A single state value registered through ``Actor.use_state``."""

    store_name: str | None
    key: str
    value: dict[str, Any]
    version: int = 0
    last_digest: str | None = None


def _canonical_json(value: Any) -> str:
    """Serialize a JSON-compatible value into a stable canonical representation."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _content_digest(value: Any) -> str:
    """Compute the sha256 digest of the canonical JSON representation of a value."""
    return hashlib.sha256(_canonical_json(value).encode('utf-8')).hexdigest()


def _parse_manifest(raw: Any) -> dict[str, Any] | None:
    """Validate a raw manifest value loaded from the key-value store.

    Returns the manifest when it has the expected shape, otherwise ``None``.
    """
    if not isinstance(raw, dict):
        return None
    if raw.get('manifestVersion') != MANIFEST_VERSION:
        return None
    if not isinstance(raw.get('generation'), int) or not isinstance(raw.get('ownerId'), str):
        return None
    if raw.get('phase') not in {'staged', 'promoted'}:
        return None
    entries = raw.get('entries')
    if not isinstance(entries, list):
        return None
    for entry in entries:
        if not isinstance(entry, dict):
            return None
        if not all(isinstance(entry.get(field), str) for field in ('key', 'tempKey', 'digest')):
            return None
        if 'storeName' in entry and not isinstance(entry['storeName'], str | None):
            return None
        if not isinstance(entry.get('version'), int):
            return None
    return raw


def _entry_matches(entry: dict[str, Any], *, store_name: str | None, key: str) -> bool:
    """Check whether a manifest entry describes the given store and state key."""
    return entry['storeName'] == store_name and entry['key'] == key


class StateCheckpointManager:
    """Coordinates atomic persistence of all states registered via ``Actor.use_state``."""

    def __init__(self, actor: _ActorType) -> None:
        """Initialize a new manager bound to an Actor instance.

        Args:
            actor: The active Actor instance, used to open key-value stores and the event manager.
        """
        self._actor = actor
        self._states: dict[tuple[str | None, str], _ManagedState] = {}
        self._stores: dict[str | None, KeyValueStore] = {}
        self._lock = asyncio.Lock()
        self._owner_id = uuid.uuid4().hex
        self._generation = 0
        self._manifest: dict[str, Any] | None = None
        self._recovered_stores: set[str | None] = set()
        self._verified_entries: set[int] = set()
        self._pending: dict[str, Any] | None = None
        self._initialized = False

    async def register(
        self,
        default_value: MutableMapping[str, JsonSerializable] | None,
        *,
        key: str,
        kvs_name: str | None,
    ) -> MutableMapping[str, JsonSerializable]:
        """Register a state and return its mutable value.

        Repeated calls with the same store and key return the same value object. Before the value
        is loaded, the store is reconciled against the last complete checkpoint so that values
        persisted by a previous process are promoted out of their temporary keys and orphaned
        temporary values are cleaned up.

        Args:
            default_value: The value to use when no persisted state exists.
            key: The canonical key of the state in the key-value store.
            kvs_name: Name of the key-value store, or ``None`` for the default store.

        Returns:
            The mutable state dictionary.

        Raises:
            TypeError: If the persisted state is not a JSON object.
        """
        async with self._lock:
            await self._initialize()

            identity = (kvs_name, key)
            existing = self._states.get(identity)
            if existing is not None:
                return existing.value

            kvs = await self._open_store(kvs_name)
            await self._recover_store(kvs_name, kvs)

            stored = await kvs.get_value(key)
            if stored is None:
                value: dict[str, Any] = copy.deepcopy(dict(default_value) if default_value is not None else {})
            elif isinstance(stored, dict):
                value = stored
            else:
                raise TypeError(
                    f'The persisted Actor state under key "{key}" in key-value store '
                    f'{kvs_name or "<default>"} is not a JSON object.'
                )

            managed = _ManagedState(store_name=kvs_name, key=key, value=value)
            version, digest = self._versioned_digest_for(kvs_name, key)
            managed.version = version
            managed.last_digest = digest
            self._states[identity] = managed
            return value

    async def persist(self, event_data: EventPersistStateData | None = None) -> None:
        """Persist all registered states as one checkpoint.

        This listener is invoked on periodic and migration-related ``PERSIST_STATE`` events.
        Failures are logged instead of propagating through the event dispatch.
        """
        try:
            await self.flush(reason='persist-state-event')
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                'Failed to persist Actor state checkpoint',
                extra={'is_migrating': event_data.is_migrating if event_data is not None else None},
            )

    async def flush(self, *, reason: str) -> None:
        """Persist all registered states as a single atomic checkpoint.

        Args:
            reason: Why the flush was triggered (used in log messages).

        Raises:
            StateCheckpointError: If the checkpoint could not be fully persisted and confirmed.
            StaleCheckpointError: If a concurrently published newer generation must win.
        """
        async with self._lock:
            await self._initialize()
            if not self._states:
                return

            logger.debug('Persisting Actor state checkpoint', extra={'reason': reason})

            if self._pending is not None:
                await self._commit_pending()

            frozen = [
                (state, copy.deepcopy(state.value), _content_digest(state.value)) for state in self._states.values()
            ]
            if all(state.last_digest == digest for state, _value, digest in frozen):
                logger.debug('All Actor states unchanged, skipping checkpoint persistence')
                return

            store_names = {state.store_name for state, _value, _digest in frozen}
            if len(store_names) == 1:
                await self._persist_single_store(frozen)
            else:
                await self._persist_with_manifest(frozen)

    async def _initialize(self) -> None:
        """Load the current manifest, reconcile referenced stores and register the event listener."""
        if self._initialized:
            return

        default_kvs = await self._open_store(None)
        self._manifest = _parse_manifest(await default_kvs.get_value(MANIFEST_KEY))
        if self._manifest is not None:
            self._generation = int(self._manifest['generation'])

        # Restore every store referenced by the manifest, even if its use_state call comes later.
        if self._manifest is not None:
            for store_name in self._manifest_store_names():
                try:
                    kvs = await self._open_store(store_name)
                    await self._recover_store(store_name, kvs)
                except Exception:
                    logger.exception(
                        'Failed to reconcile key-value store while restoring Actor state',
                        extra={'kvs_name': store_name},
                    )

        self._actor.event_manager.on(event=Event.PERSIST_STATE, listener=self.persist)
        self._initialized = True

    async def _open_store(self, name: str | None) -> KeyValueStore:
        """Open a key-value store by name, caching the instance."""
        if name not in self._stores:
            self._stores[name] = await self._actor.open_key_value_store(name=name)
        return self._stores[name]

    def _manifest_store_names(self) -> list[str | None]:
        """Return the distinct store names referenced by the current manifest."""
        if self._manifest is None:
            return []
        names: list[str | None] = []
        for entry in self._manifest['entries']:
            store_name = entry['storeName']
            if store_name not in names:
                names.append(store_name)
        return names

    def _versioned_digest_for(self, store_name: str | None, key: str) -> tuple[int, str | None]:
        """Return the content version and digest of a verified manifest entry."""
        if self._manifest is None:
            return 0, None
        for index, entry in enumerate(self._manifest['entries']):
            if index in self._verified_entries and _entry_matches(entry, store_name=store_name, key=key):
                return int(entry['version']), str(entry['digest'])
        return 0, None

    async def _recover_store(self, name: str | None, kvs: KeyValueStore) -> None:
        """Promote complete-generation values and delete orphaned temporary keys in one store."""
        if name in self._recovered_stores:
            return
        # Any in-flight staging from this process is superseded by a register boundary.
        self._pending = None

        entries: list[tuple[int, dict[str, Any]]] = []
        referenced_temp_keys: set[str] = set()
        if self._manifest is not None:
            for index, entry in enumerate(self._manifest['entries']):
                if entry['storeName'] == name:
                    entries.append((index, entry))
                    referenced_temp_keys.add(str(entry['tempKey']))

        temp_keys = {metadata.key async for metadata in kvs.iterate_keys() if metadata.key.startswith(TEMP_KEY_PREFIX)}

        for index, entry in entries:
            await self._recover_entry(kvs, index, entry)

        for orphan_key in temp_keys - referenced_temp_keys:
            await self._delete_quietly(kvs, orphan_key, what='orphaned checkpoint value')

        self._recovered_stores.add(name)
        await self._finalize_recovery_if_complete()

    async def _recover_entry(self, kvs: KeyValueStore, index: int, entry: dict[str, Any]) -> None:
        """Verify one manifest entry and promote its temporary value when necessary."""
        expected_digest = str(entry['digest'])
        canonical = await kvs.get_value(str(entry['key']))
        if isinstance(canonical, dict) and _content_digest(canonical) == expected_digest:
            self._verified_entries.add(index)
            return

        staged = await kvs.get_value(str(entry['tempKey']))
        if not isinstance(staged, dict) or _content_digest(staged) != expected_digest:
            logger.warning(
                'Cannot restore Actor state entry from the last checkpoint generation',
                extra={'kvs_name': entry['storeName'], 'key': entry['key'], 'generation': self._generation},
            )
            return

        await self._set_json_with_attempts(kvs, str(entry['key']), staged)
        promoted = await kvs.get_value(str(entry['key']))
        if isinstance(promoted, dict) and _content_digest(promoted) == expected_digest:
            self._verified_entries.add(index)
        else:
            logger.warning(
                'Promotion of an Actor state entry could not be confirmed',
                extra={'kvs_name': entry['storeName'], 'key': entry['key'], 'generation': self._generation},
            )

    async def _finalize_recovery_if_complete(self) -> None:
        """Flip a fully restored manifest to promoted and remove its temporary keys."""
        manifest = self._manifest
        if manifest is None or len(self._verified_entries) != len(manifest['entries']):
            return

        if manifest['phase'] != 'promoted':
            manifest['phase'] = 'promoted'
            await self._publish_manifest(manifest)
            self._manifest = manifest

        for entry in manifest['entries']:
            kvs = self._stores.get(entry['storeName'])
            if kvs is not None:
                await self._delete_quietly(kvs, str(entry['tempKey']), what='restored checkpoint value')

    async def _persist_single_store(
        self,
        frozen: list[tuple[_ManagedState, dict[str, Any], str]],
    ) -> None:
        """Persist states directly to their canonical key, preserving the legacy single-store format."""
        store_name = frozen[0][0].store_name
        kvs = await self._open_store(store_name)
        for state, value, digest in frozen:
            await self._set_json_with_attempts(kvs, state.key, value)
            if state.last_digest != digest:
                state.version += 1
            state.last_digest = digest

    async def _persist_with_manifest(
        self,
        frozen: list[tuple[_ManagedState, dict[str, Any], str]],
    ) -> None:
        """Freeze, stage, publish and promote a multi-store checkpoint generation."""
        await self._guard_against_newer_generation()

        generation = self._generation + 1
        entries: list[dict[str, Any]] = []
        items: list[dict[str, Any]] = []
        for index, (state, value, digest) in enumerate(frozen):
            version = state.version + (1 if state.last_digest != digest else 0)
            entry = {
                'storeName': state.store_name,
                'key': state.key,
                'tempKey': f'{TEMP_KEY_PREFIX}{generation}/{self._owner_id}/{index}',
                'digest': digest,
                'version': version,
            }
            entries.append(entry)
            items.append({'state': state, 'value': value, 'entry': entry, 'staged_value': None})

        manifest = {
            'manifestVersion': MANIFEST_VERSION,
            'generation': generation,
            'ownerId': self._owner_id,
            'createdAt': datetime.now(UTC).isoformat(),
            'phase': 'staged',
            'entries': entries,
        }

        await self._stage_items(items)
        self._pending = {'manifest': manifest, 'items': items}
        await self._commit_pending()

    async def _guard_against_newer_generation(self) -> None:
        """Abort the flush when a different owner already published a newer generation."""
        default_kvs = await self._open_store(None)
        remote = _parse_manifest(await default_kvs.get_value(MANIFEST_KEY))
        if remote is not None and int(remote['generation']) > self._generation and remote['ownerId'] != self._owner_id:
            raise StaleCheckpointError(
                f'A newer Actor state checkpoint (generation {remote["generation"]}) was published by another owner; '
                f'keeping it instead of generation {self._generation + 1} from this owner.'
            )

    async def _stage_items(self, items: list[dict[str, Any]]) -> None:
        """Write every entry to its temporary key, retrying only entries that are not yet confirmed."""
        pending_items = list(items)
        for _attempt in range(_WRITE_ATTEMPTS):
            results = await asyncio.gather(*[self._stage_one(item) for item in pending_items], return_exceptions=True)
            pending_items = [
                item
                for item, result in zip(pending_items, results, strict=True)
                if result is not True or item['staged_value'] is None
            ]
            if not pending_items:
                return

        raise StateCheckpointError(
            'Failed to stage all Actor state values, keeping the previous complete checkpoint generation'
        )

    async def _stage_one(self, item: dict[str, Any]) -> bool:
        """Write one entry to its temporary key unless the expected value is already staged there."""
        state: _ManagedState = item['state']
        entry: dict[str, Any] = item['entry']
        value: dict[str, Any] = item['value']
        expected_digest = str(entry['digest'])
        kvs = await self._open_store(state.store_name)

        staged = await kvs.get_value(str(entry['tempKey']))
        if not isinstance(staged, dict) or _content_digest(staged) != expected_digest:
            await self._set_json_with_attempts(kvs, str(entry['tempKey']), value)
            staged = await kvs.get_value(str(entry['tempKey']))

        if isinstance(staged, dict) and _content_digest(staged) == expected_digest:
            item['staged_value'] = staged
            return True
        return False

    async def _commit_pending(self) -> None:
        """Confirm staged values, publish the manifest, promote and clean up a pending generation."""
        pending = self._pending
        if pending is None:
            return
        manifest: dict[str, Any] = pending['manifest']
        items: list[dict[str, Any]] = pending['items']

        try:
            await self._stage_items(items)
            await self._publish_manifest(manifest)
            await self._promote_items(items)

            promoted_manifest = {**manifest, 'phase': 'promoted'}
            await self._publish_manifest(promoted_manifest)
            manifest['phase'] = 'promoted'
        except StaleCheckpointError:
            # A newer snapshot won; this owner must leave its staging behind and stop writing.
            self._pending = None
            raise

        for item in items:
            state: _ManagedState = item['state']
            entry: dict[str, Any] = item['entry']
            state.version = int(entry['version'])
            state.last_digest = str(entry['digest'])
            kvs = await self._open_store(state.store_name)
            await self._delete_quietly(kvs, str(entry['tempKey']), what='committed checkpoint value')

        self._generation = int(manifest['generation'])
        self._manifest = manifest
        self._verified_entries = set(range(len(manifest['entries'])))
        self._pending = None

    async def _promote_items(self, items: list[dict[str, Any]]) -> None:
        """Promote staged values to their canonical keys, retrying unconfirmed promotions."""
        pending_items = list(items)
        for _attempt in range(_WRITE_ATTEMPTS):
            results = await asyncio.gather(*[self._promote_one(item) for item in pending_items], return_exceptions=True)
            pending_items = [item for item, result in zip(pending_items, results, strict=True) if result is not True]
            if not pending_items:
                return

        raise StateCheckpointError(
            'Actor state checkpoint manifest was published but promotion of canonical values could not be confirmed'
        )

    async def _promote_one(self, item: dict[str, Any]) -> bool:
        """Write one staged value to its canonical key and confirm the content."""
        state: _ManagedState = item['state']
        entry: dict[str, Any] = item['entry']
        staged_value: dict[str, Any] | None = item['staged_value']
        expected_digest = str(entry['digest'])
        if staged_value is None:
            return False

        kvs = await self._open_store(state.store_name)
        await self._set_json_with_attempts(kvs, state.key, staged_value)
        canonical = await kvs.get_value(state.key)
        return isinstance(canonical, dict) and _content_digest(canonical) == expected_digest

    async def _publish_manifest(self, manifest: dict[str, Any]) -> None:
        """Publish the manifest to the default store, guarding against stale owners and confirming the write."""
        default_kvs = await self._open_store(None)
        for _attempt in range(_WRITE_ATTEMPTS):
            remote = _parse_manifest(await default_kvs.get_value(MANIFEST_KEY))
            if remote is not None and int(remote['generation']) > int(manifest['generation']):
                raise StaleCheckpointError(
                    f'A newer Actor state checkpoint (generation {remote["generation"]}) is already published; '
                    f'not overwriting it with generation {manifest["generation"]}.'
                )
            if (
                remote is not None
                and int(remote['generation']) == int(manifest['generation'])
                and remote['ownerId'] != manifest['ownerId']
            ):
                raise StaleCheckpointError(
                    f'Generation {manifest["generation"]} of the Actor state checkpoint is already owned by '
                    f'another owner.'
                )

            try:
                await default_kvs.set_value(MANIFEST_KEY, manifest, JSON_CONTENT_TYPE)
            except Exception:
                # The values are safely staged, so the next attempt can just re-issue the manifest.
                logger.debug(
                    'Checkpoint manifest write failed, retrying its publication',
                    extra={'generation': manifest['generation']},
                    exc_info=True,
                )
            else:
                confirmed = _parse_manifest(await default_kvs.get_value(MANIFEST_KEY))
                if confirmed == manifest:
                    return

        raise StateCheckpointError('Actor state checkpoint manifest publication could not be confirmed')

    async def _set_json_with_attempts(self, kvs: KeyValueStore, key: str, value: Any) -> None:
        """Write a JSON value to a key with retries."""
        last_error: Exception | None = None
        for _attempt in range(_WRITE_ATTEMPTS):
            try:
                await kvs.set_value(key, value, JSON_CONTENT_TYPE)
            except Exception as exc:
                last_error = exc
            else:
                return
        raise StateCheckpointError(f'Failed to write Actor state key "{key}"') from last_error

    async def _delete_quietly(self, kvs: KeyValueStore, key: str, *, what: str) -> None:
        """Delete a key without failing the checkpoint flow when the deletion errors."""
        try:
            await kvs.delete_value(key)
        except Exception:
            logger.exception('Failed to delete %s', what, extra={'key': key})

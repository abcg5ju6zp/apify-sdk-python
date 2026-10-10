from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from apify import Actor
from apify._consts import ApifyEnvVars
from apify._state_checkpoints import (
    MANIFEST_KEY,
    TEMP_KEY_PREFIX,
    StaleCheckpointError,
    StateCheckpointError,
    _content_digest,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from crawlee.storages import KeyValueStore

    from apify._state_checkpoints import StateCheckpointManager

STATE_KEY = 'APIFY_GLOBAL_STATE'
CUSTOM_KVS = 'custom-kvs'


def _faulty_set_value(
    kvs: KeyValueStore,
    *,
    fails: Any = None,
    block: tuple[asyncio.Event, asyncio.Event] | None = None,
) -> None:
    """Patch a KVS instance with a set_value wrapper for fault injection.

    Args:
        kvs: The key-value store instance to patch.
        fails: A callable ``(key) -> bool``; keys it returns True for raise ``RuntimeError``.
        block: ``(entered, gate)`` events; the first temp-key write signals ``entered`` and waits on ``gate``.
    """
    original_set_value = kvs.set_value
    blocked = {'done': False}

    async def patched_set_value(key: str, value: Any, content_type: str | None = None) -> None:
        if fails is not None and fails(key):
            raise RuntimeError('Injected storage failure')
        if block is not None and not blocked['done'] and key.startswith(TEMP_KEY_PREFIX):
            blocked['done'] = True
            entered, gate = block
            entered.set()
            await gate.wait()
        await original_set_value(key, value, content_type)

    kvs.set_value = patched_set_value  # ty: ignore[invalid-assignment]


async def _temp_keys(kvs: KeyValueStore) -> set[str]:
    return {meta.key async for meta in kvs.iterate_keys() if meta.key.startswith(TEMP_KEY_PREFIX)}


def _manager(actor: Any) -> StateCheckpointManager:
    assert actor._state_checkpoint_manager is not None
    return actor._state_checkpoint_manager


async def test_multi_store_checkpoint_is_published_and_promoted() -> None:
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)

        state_default['value'] = 'default_store'
        state_custom['value'] = 'custom_store'

        kvs_default = await actor.open_key_value_store()
        kvs_custom = await actor.open_key_value_store(name=CUSTOM_KVS)

    saved_default = await kvs_default.get_value(STATE_KEY)
    saved_custom = await kvs_custom.get_value(STATE_KEY)
    assert saved_default == {'value': 'default_store'}
    assert saved_custom == {'value': 'custom_store'}

    manifest = await kvs_default.get_value(MANIFEST_KEY)
    assert manifest is not None
    assert manifest['manifestVersion'] == 1
    assert manifest['phase'] == 'promoted'
    assert len(manifest['entries']) == 2
    assert {entry['storeName'] for entry in manifest['entries']} == {None, CUSTOM_KVS}
    assert manifest['entries'][0]['digest'] == _content_digest(saved_default)
    assert manifest['entries'][1]['digest'] == _content_digest(saved_custom)

    assert await _temp_keys(kvs_default) == set()
    assert await _temp_keys(kvs_custom) == set()


async def test_multi_store_periodic_persist_publishes_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ApifyEnvVars.PERSIST_STATE_INTERVAL_MILLIS, '100')

    async def manifest_phase() -> str | None:
        kvs = await Actor.open_key_value_store()
        manifest = await kvs.get_value(MANIFEST_KEY)
        return manifest['phase'] if manifest else None

    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        state_default['value'] = 'periodic_default'
        state_custom['value'] = 'periodic_custom'

        from ..._utils import poll_until_condition

        phase = await poll_until_condition(manifest_phase, lambda value: value == 'promoted', poll_interval=0.05)
        assert phase == 'promoted'


async def test_single_store_keeps_legacy_format_without_checkpoint_keys() -> None:
    async with Actor as actor:
        state = await actor.use_state(default_value={'a': 1})
        state['b'] = 2
        kvs = await actor.open_key_value_store()

    all_keys = {meta.key async for meta in kvs.iterate_keys()}
    assert all_keys == {STATE_KEY}
    assert await kvs.get_value(STATE_KEY) == {'a': 1, 'b': 2}
    assert await kvs.get_value(MANIFEST_KEY) is None


async def test_actor_without_use_state_is_unaffected() -> None:
    async with Actor as actor:
        await actor.set_value('USER_KEY', {'hello': 'world'})
        assert actor._state_checkpoint_manager is None
        kvs = await actor.open_key_value_store()

    assert await kvs.get_value('USER_KEY') == {'hello': 'world'}
    assert await kvs.get_value(MANIFEST_KEY) is None
    assert await _temp_keys(kvs) == set()


async def test_use_state_default_value_is_copied_and_identity_is_stable() -> None:
    async with Actor as actor:
        default: dict[str, Any] = {'x': 1}
        state = await actor.use_state(default)
        assert state == {'x': 1}

        default['x'] = 2
        assert state == {'x': 1}

        assert await actor.use_state() is state


async def test_failed_store_write_keeps_previous_generation_and_restores_it(
    monkeypatch: pytest.MonkeyPatch,
    prepare_test_env: Callable[[], None],
) -> None:
    # Simulate platform behavior: the default KVS survives the container restart.
    monkeypatch.setenv(ApifyEnvVars.PURGE_ON_START, 'false')

    # Run 1: publish a complete generation across two stores.
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        state_default['gen'] = 1
        state_custom['gen'] = 1
        kvs_default = await actor.open_key_value_store()
        kvs_custom = await actor.open_key_value_store(name=CUSTOM_KVS)

    assert (await kvs_default.get_value(MANIFEST_KEY))['generation'] == 1

    # Run 2: staging the second generation fails permanently for one store.
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        assert state_default == {'gen': 1}
        assert state_custom == {'gen': 1}
        state_default['gen'] = 2

        faulty_kvs = await actor.open_key_value_store(name=CUSTOM_KVS)
        _faulty_set_value(faulty_kvs, fails=lambda key: key.startswith(TEMP_KEY_PREFIX))

        with pytest.raises(StateCheckpointError):
            await _manager(actor).flush(reason='test')

        # The manifest must still describe the first, complete generation.
        manifest = await (await actor.open_key_value_store()).get_value(MANIFEST_KEY)
        assert manifest['generation'] == 1
        assert manifest['phase'] == 'promoted'

    # Run 3: a brand new process (fresh Actor instance) restores from generation 1.
    prepare_test_env()
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        assert state_default == {'gen': 1}
        assert state_custom == {'gen': 1}

        kvs_default = await actor.open_key_value_store()
        kvs_custom = await actor.open_key_value_store(name=CUSTOM_KVS)

    assert await _temp_keys(kvs_default) == set()
    assert await _temp_keys(kvs_custom) == set()
    assert await kvs_default.get_value(STATE_KEY) == {'gen': 1}
    assert await kvs_custom.get_value(STATE_KEY) == {'gen': 1}
    assert (await kvs_default.get_value(MANIFEST_KEY))['generation'] == 1


async def test_manifest_publish_failure_is_resent_after_digest_recheck() -> None:
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        state_default['value'] = 'a'
        state_custom['value'] = 'b'

        kvs_default = await actor.open_key_value_store()
        failures = {'count': 0}

        original_set_value = kvs_default.set_value

        async def flaky_manifest_write(key: str, value: Any, content_type: str | None = None) -> None:
            if key == MANIFEST_KEY:
                failures['count'] += 1
                if failures['count'] <= 3:
                    raise RuntimeError('Manifest API temporarily unavailable')
            await original_set_value(key, value, content_type)

        kvs_default.set_value = flaky_manifest_write  # ty: ignore[invalid-assignment]

        # First attempt stages all values but cannot publish the manifest.
        with pytest.raises(StateCheckpointError):
            await _manager(actor).flush(reason='test')

        manifest_after_failure = await kvs_default.get_value(MANIFEST_KEY)
        assert manifest_after_failure is None
        assert await _temp_keys(kvs_default) != set()

        # The second attempt reuses the staged values (digest recheck) and publishes the manifest.
        kvs_default.set_value = original_set_value  # ty: ignore[invalid-assignment]
        await _manager(actor).flush(reason='test-retry')

        manifest = await kvs_default.get_value(MANIFEST_KEY)
        assert manifest is not None
        assert manifest['phase'] == 'promoted'
        assert len(manifest['entries']) == 2
        assert await kvs_default.get_value(STATE_KEY) == {'value': 'a'}
        assert await (await actor.open_key_value_store(name=CUSTOM_KVS)).get_value(STATE_KEY) == {'value': 'b'}
        assert await _temp_keys(kvs_default) == set()


async def test_stale_owner_cannot_overwrite_newer_generation() -> None:
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        state_default['gen'] = 1
        state_custom['gen'] = 1
        await _manager(actor).flush(reason='test')

        kvs_default = await actor.open_key_value_store()
        kvs_custom = await actor.open_key_value_store(name=CUSTOM_KVS)

        # A new container (different owner) already published generation 2.
        newer_manifest = await kvs_default.get_value(MANIFEST_KEY)
        newer_manifest['generation'] = 2
        newer_manifest['ownerId'] = 'another-owner'
        await kvs_default.set_value(MANIFEST_KEY, newer_manifest)

        state_default['gen'] = 2
        state_custom['gen'] = 2

        with pytest.raises(StaleCheckpointError):
            await _manager(actor).flush(reason='reboot-race')

        # The stale owner published nothing and the canonical values stay at the older snapshot.
        manifest = await kvs_default.get_value(MANIFEST_KEY)
        assert manifest['generation'] == 2
        assert manifest['ownerId'] == 'another-owner'
        assert await kvs_default.get_value(STATE_KEY) == {'gen': 1}
        assert await kvs_custom.get_value(STATE_KEY) == {'gen': 1}
        assert await _temp_keys(kvs_default) == set()
        assert await _temp_keys(kvs_custom) == set()


async def test_state_modified_during_flush_is_saved_in_next_generation() -> None:
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        state_default['value'] = 'frozen'
        state_custom['value'] = 'custom'

        kvs_default = await actor.open_key_value_store()
        kvs_custom = await actor.open_key_value_store(name=CUSTOM_KVS)

        entered = asyncio.Event()
        gate = asyncio.Event()
        _faulty_set_value(kvs_default, block=(entered, gate))

        flush_task = asyncio.create_task(_manager(actor).flush(reason='test'))
        await entered.wait()
        # Mutate the state while generation 1 is being staged - it must not leak into generation 1.
        state_default['value'] = 'mutated_during_save'
        gate.set()
        await flush_task

        assert await kvs_default.get_value(STATE_KEY) == {'value': 'frozen'}
        manifest = await kvs_default.get_value(MANIFEST_KEY)
        assert manifest['generation'] == 1

        # The mutation is persisted in the next generation.
        await _manager(actor).flush(reason='test-next')
        assert await kvs_default.get_value(STATE_KEY) == {'value': 'mutated_during_save'}
        assert await kvs_custom.get_value(STATE_KEY) == {'value': 'custom'}
        assert (await kvs_default.get_value(MANIFEST_KEY))['generation'] == 2


async def test_cancelled_flush_is_not_reported_as_success() -> None:
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        state_default['value'] = 'default'
        state_custom['value'] = 'custom'

        kvs_default = await actor.open_key_value_store()

        entered = asyncio.Event()
        gate = asyncio.Event()
        _faulty_set_value(kvs_default, block=(entered, gate))

        flush_task = asyncio.create_task(_manager(actor).flush(reason='test'))
        await entered.wait()
        flush_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await flush_task
        gate.set()

        # No manifest, no promoted canonical values - the half-written snapshot is not a success.
        assert await kvs_default.get_value(MANIFEST_KEY) is None
        assert await kvs_default.get_value(STATE_KEY) is None

        # A follow-up flush completes the snapshot normally.
        await _manager(actor).flush(reason='test-retry')
        assert await kvs_default.get_value(STATE_KEY) == {'value': 'default'}
        assert await (await actor.open_key_value_store(name=CUSTOM_KVS)).get_value(STATE_KEY) == {'value': 'custom'}
        manifest = await kvs_default.get_value(MANIFEST_KEY)
        assert manifest['phase'] == 'promoted'


async def test_restart_promotes_staged_manifest_and_cleans_temps(
    monkeypatch: pytest.MonkeyPatch,
    prepare_test_env: Callable[[], None],
) -> None:
    # Simulate platform behavior: the default KVS survives the container restart.
    monkeypatch.setenv(ApifyEnvVars.PURGE_ON_START, 'false')

    default_value = {'restored': 'default', 'n': 1}
    custom_value = {'restored': 'custom', 'n': 2}

    # Simulate a crash after manifest publication but before promotion.
    async with Actor as actor:
        kvs_default = await actor.open_key_value_store()
        kvs_custom = await actor.open_key_value_store(name=CUSTOM_KVS)

        default_temp = f'{TEMP_KEY_PREFIX}5/old-owner/0'
        custom_temp = f'{TEMP_KEY_PREFIX}5/old-owner/1'
        await kvs_default.set_value(default_temp, default_value, 'application/json')
        await kvs_custom.set_value(custom_temp, custom_value, 'application/json')
        # An orphaned temp from an even older, never-published generation.
        await kvs_custom.set_value(f'{TEMP_KEY_PREFIX}4/old-owner/1', {'stale': True}, 'application/json')

        manifest = {
            'manifestVersion': 1,
            'generation': 5,
            'ownerId': 'old-owner',
            'createdAt': '2026-10-10T00:00:00+00:00',
            'phase': 'staged',
            'entries': [
                {
                    'storeName': None,
                    'key': STATE_KEY,
                    'tempKey': default_temp,
                    'digest': _content_digest(default_value),
                    'version': 3,
                },
                {
                    'storeName': CUSTOM_KVS,
                    'key': STATE_KEY,
                    'tempKey': custom_temp,
                    'digest': _content_digest(custom_value),
                    'version': 3,
                },
            ],
        }
        await kvs_default.set_value(MANIFEST_KEY, manifest, 'application/json')

    # New run (fresh process): all stores are restored from the last complete generation.
    prepare_test_env()
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        assert state_default == default_value
        assert state_custom == custom_value

        kvs_default = await actor.open_key_value_store()
        kvs_custom = await actor.open_key_value_store(name=CUSTOM_KVS)

    assert await kvs_default.get_value(STATE_KEY) == default_value
    assert await kvs_custom.get_value(STATE_KEY) == custom_value
    assert await _temp_keys(kvs_default) == set()
    assert await _temp_keys(kvs_custom) == set()

    recovered_manifest = await kvs_default.get_value(MANIFEST_KEY)
    assert recovered_manifest['phase'] == 'promoted'
    assert recovered_manifest['generation'] == 5


async def test_concurrent_periodic_and_migration_flushes_commit_one_consistent_snapshot() -> None:
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        state_default['value'] = 'default'
        state_custom['value'] = 'custom'

        manager = _manager(actor)
        await asyncio.gather(
            manager.flush(reason='periodic'),
            manager.flush(reason='migration'),
            manager.flush(reason='exit'),
        )

        kvs_default = await actor.open_key_value_store()
        kvs_custom = await actor.open_key_value_store(name=CUSTOM_KVS)

    manifest = await kvs_default.get_value(MANIFEST_KEY)
    assert manifest['generation'] == 1
    assert manifest['phase'] == 'promoted'
    assert await kvs_default.get_value(STATE_KEY) == {'value': 'default'}
    assert await kvs_custom.get_value(STATE_KEY) == {'value': 'custom'}
    assert await _temp_keys(kvs_default) == set()
    assert await _temp_keys(kvs_custom) == set()


async def test_new_generation_after_reboot_restores_and_then_advances(
    monkeypatch: pytest.MonkeyPatch,
    prepare_test_env: Callable[[], None],
) -> None:
    # Simulate platform behavior: the default KVS survives the container restart.
    monkeypatch.setenv(ApifyEnvVars.PURGE_ON_START, 'false')

    # Run 1: complete generation across two stores.
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        state_default['gen'] = 1
        state_custom['gen'] = 1

    # Run 2 (new container, new owner): recovery adopts generation 1 and advances to generation 2.
    prepare_test_env()
    async with Actor as actor:
        state_default = await actor.use_state()
        state_custom = await actor.use_state(kvs_name=CUSTOM_KVS)
        assert state_default == {'gen': 1}
        assert state_custom == {'gen': 1}

        state_default['gen'] = 2
        state_custom['gen'] = 2
        kvs_default = await actor.open_key_value_store()
        kvs_custom = await actor.open_key_value_store(name=CUSTOM_KVS)

    manifest = await kvs_default.get_value(MANIFEST_KEY)
    assert manifest['generation'] == 2
    assert manifest['phase'] == 'promoted'
    assert await kvs_default.get_value(STATE_KEY) == {'gen': 2}
    assert await kvs_custom.get_value(STATE_KEY) == {'gen': 2}
    assert await _temp_keys(kvs_default) == set()
    assert await _temp_keys(kvs_custom) == set()

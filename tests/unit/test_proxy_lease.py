from __future__ import annotations

import copy
from collections import defaultdict
from datetime import timedelta
from typing import Any

import pytest

from apify._proxy_lease import (
    LEASE_CHECKPOINT_KEY,
    QUARANTINED_LEASE_STATE,
    ProxyExitUncertainError,
    ProxyLeaseManager,
    ProxyLeaseOwnershipLostError,
    ProxyLeasePersistError,
    ProxyLeaseStore,
    ProxyNotReadyError,
    build_physical_session_id,
)

RUN_ID = 'RUN-1'
PASSWORD = 'PROXY_PASSWORD'


def status_ok(exit_id: str) -> dict[str, Any]:
    return {'connected': True, 'connectionError': None, 'isManInTheMiddle': False, 'clientIp': exit_id}


STATUS_DOWN: dict[str, Any] = {
    'connected': False,
    'connectionError': 'DUMMY_CONNECTION_ERROR',
    'isManInTheMiddle': False,
}


class InMemoryKvs:
    """Key-value store stand-in that emulates a JSON serialization boundary."""

    def __init__(self, *, save_error: Exception | None = None) -> None:
        self._data: dict[str, Any] = {}
        self._save_error = save_error
        self.save_count = 0
        self.get_count = 0

    async def get_value(self, key: str) -> Any:
        self.get_count += 1
        return copy.deepcopy(self._data.get(key))

    async def set_value(self, key: str, value: Any) -> None:
        self.save_count += 1
        if self._save_error is not None:
            raise self._save_error
        self._data[key] = copy.deepcopy(value)

    def checkpoint(self) -> dict[str, Any]:
        return copy.deepcopy(self._data[LEASE_CHECKPOINT_KEY])

    def lease(self, logical_session_id: str) -> dict[str, Any]:
        return self.checkpoint()['leases'][logical_session_id]


class StatusScript:
    """Scripted proxy status responses keyed by the physical proxy session in the requested URL."""

    def __init__(self) -> None:
        self.queues: dict[str, list[dict[str, Any] | None]] = defaultdict(list)
        self.queries: list[str] = []

    def queue(self, physical_session_id: str, *results: dict[str, Any] | None) -> None:
        self.queues[physical_session_id].extend(results)

    async def fetch(self, proxy_url: str) -> dict[str, Any] | None:
        physical_session_id = proxy_url.split('@', 1)[0].removeprefix('http://').split(':', 1)[0]
        physical_session_id = physical_session_id.removeprefix('session-')
        self.queries.append(physical_session_id)
        queue = self.queues[physical_session_id]
        if not queue:
            raise AssertionError(f'Unexpected proxy status query for physical session {physical_session_id!r}')
        return queue.pop(0)


def make_manager(
    kvs: InMemoryKvs,
    status: StatusScript,
    *,
    password: str | None = PASSWORD,
    password_error: Exception | None = None,
    owner_run_id: str | None = RUN_ID,
    groups: list[str] | None = None,
    country_code: str | None = None,
) -> ProxyLeaseManager:
    async def ensure_password() -> str:
        if password_error is not None:
            raise password_error
        return password or ''

    def build_proxy_url(physical_session_id: str, _proxy_tier: int | None) -> str:
        return f'http://session-{physical_session_id}:{PASSWORD}@proxy.example:8000'

    return ProxyLeaseManager(
        owner_run_id=owner_run_id,
        groups=groups or ['RESIDENTIAL'],
        country_code=country_code,
        subdivision_code=None,
        store=ProxyLeaseStore(lambda: _return_kvs(kvs)),
        ensure_password=ensure_password,
        build_proxy_url=build_proxy_url,
        fetch_proxy_status=status.fetch,
        verify_interval=timedelta(0),
    )


async def _return_kvs(kvs: InMemoryKvs) -> InMemoryKvs:
    return kvs


def test_build_physical_session_id() -> None:
    assert build_physical_session_id('sess1', 1) == 'sess1'
    assert build_physical_session_id('sess1', 2) == 'sess1~g2'
    long_session = 'x' * 50
    physical = build_physical_session_id(long_session, 2)
    assert len(physical) == 50
    assert physical.endswith('~g2')
    assert physical != long_session


async def test_fresh_lease_is_created_persisted_and_confirmed() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'))
    manager = make_manager(kvs, status)

    lease, rotated = await manager.issue('sess1', None)

    assert rotated is False
    assert lease.generation == 1
    assert lease.epoch == 1
    assert lease.physical_session_id == 'sess1'
    assert lease.exit_id == '1.1.1.1'
    assert lease.owner_run_id == RUN_ID
    assert lease.groups == ['RESIDENTIAL']
    assert lease.username == 'session-sess1'

    persisted = kvs.lease('sess1')
    assert persisted['generation'] == 1
    assert persisted['state'] == 'active'
    assert persisted['exit_id'] == '1.1.1.1'


async def test_second_issue_reuses_confirmed_lease_without_re_rotation() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'), status_ok('1.1.1.1'))
    manager = make_manager(kvs, status)

    lease_1, rotated_1 = await manager.issue('sess1', None)
    lease_2, rotated_2 = await manager.issue('sess1', None)

    assert (lease_1.physical_session_id, rotated_1) == ('sess1', False)
    assert (lease_2.physical_session_id, rotated_2) == ('sess1', False)
    assert status.queries == ['sess1', 'sess1']


async def test_reboot_takeover_restores_original_exit() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'))
    old_manager = make_manager(kvs, status)
    old_lease, old_rotated = await old_manager.issue('sess1', None)
    assert old_rotated is False

    # A new process after migration/reboot opens the same run-scoped checkpoint.
    status.queue('sess1', status_ok('1.1.1.1'))
    new_manager = make_manager(kvs, status)
    restored_lease, rotated = await new_manager.issue('sess1', None)

    assert rotated is False
    assert restored_lease.generation == 1
    assert restored_lease.epoch == 2
    assert restored_lease.physical_session_id == 'sess1'
    assert restored_lease.exit_id == '1.1.1.1'
    assert restored_lease.owner_token != old_lease.owner_token

    # The original physical exit was re-verified by the new process.
    assert status.queries == ['sess1', 'sess1']


async def test_old_process_is_fenced_out_after_takeover() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'), status_ok('1.1.1.1'))
    old_manager = make_manager(kvs, status)
    await old_manager.issue('sess1', None)

    new_manager = make_manager(kvs, status)
    await new_manager.issue('sess1', None)

    # The old process confirms owner/epoch before every issuance and must not sign any more URLs.
    with pytest.raises(ProxyLeaseOwnershipLostError):
        await old_manager.issue('sess1', None)


async def test_silently_switched_exit_is_quarantined_and_rotated() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'))
    old_manager = make_manager(kvs, status)
    await old_manager.issue('sess1', None)

    # After the reboot the sticky session silently resolves to a different exit.
    status.queue('sess1', status_ok('9.9.9.9'))
    status.queue('sess1~g2', status_ok('9.9.9.9'))
    new_manager = make_manager(kvs, status)
    lease, rotated = await new_manager.issue('sess1', None)

    assert rotated is True
    assert lease.generation == 2
    assert lease.physical_session_id == 'sess1~g2'
    assert lease.exit_id == '9.9.9.9'
    assert lease.previous_exit_id == '1.1.1.1'

    persisted = kvs.lease('sess1')
    assert persisted['generation'] == 2
    assert persisted['state'] == 'active'
    assert persisted['previous_exit_id'] == '1.1.1.1'
    assert status.queries == ['sess1', 'sess1', 'sess1~g2']


async def test_disconnected_exit_triggers_rotation() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'))
    manager_a = make_manager(kvs, status)
    await manager_a.issue('sess1', None)

    status.queue('sess1', STATUS_DOWN)
    status.queue('sess1~g2', status_ok('5.5.5.5'))
    manager_b = make_manager(kvs, status)
    lease, rotated = await manager_b.issue('sess1', None)

    assert rotated is True
    assert lease.generation == 2
    assert lease.physical_session_id == 'sess1~g2'
    assert lease.exit_id == '5.5.5.5'


async def test_uncertain_status_keeps_original_exit_and_fails_closed() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'))
    manager_a = make_manager(kvs, status)
    await manager_a.issue('sess1', None)

    # Two attempts per verification, both indeterminate; the original exit must be re-verified, not rotated.
    status.queue('sess1', None, None)
    manager_b = make_manager(kvs, status)
    with pytest.raises(ProxyExitUncertainError) as exc_info:
        await manager_b.issue('sess1', None)
    assert exc_info.value.physical_session_id == 'sess1'

    # The takeover was committed (epoch bumped) but no rotation happened.
    persisted = kvs.lease('sess1')
    assert persisted['generation'] == 1
    assert persisted['physical_session_id'] == 'sess1'
    assert status.queries == ['sess1', 'sess1', 'sess1']

    # The next request re-verifies the original exit and continues from the checkpoint.
    status.queue('sess1', status_ok('1.1.1.1'))
    lease, rotated = await manager_b.issue('sess1', None)
    assert rotated is False
    assert lease.physical_session_id == 'sess1'


async def test_missing_password_keeps_proxy_not_ready() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    manager = make_manager(kvs, status, password='')

    with pytest.raises(ProxyNotReadyError):
        await manager.issue('sess1', None)

    assert not kvs._data  # no checkpoint write happened at all


async def test_persist_failure_on_claim_rejects_issuance() -> None:
    kvs = InMemoryKvs(save_error=RuntimeError('KVS down'))
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'))
    manager = make_manager(kvs, status)

    with pytest.raises(ProxyLeasePersistError):
        await manager.issue('sess1', None)

    assert status.queries == []  # no status query before the lease was safely persisted


async def test_handover_persist_failure_rejects_handover() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'))
    manager = make_manager(kvs, status)
    await manager.issue('sess1', None)

    kvs._save_error = RuntimeError('KVS down')
    with pytest.raises(ProxyLeasePersistError):
        await manager.handover()


async def test_handover_succeeds_and_restarts_from_checkpoint() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'), status_ok('1.1.1.1'))
    manager_a = make_manager(kvs, status)
    await manager_a.issue('sess1', None)
    await manager_a.handover()

    assert kvs.lease('sess1')['generation'] == 1

    manager_b = make_manager(kvs, status)
    lease, rotated = await manager_b.issue('sess1', None)
    assert rotated is False
    assert lease.exit_id == '1.1.1.1'


async def test_identity_change_rotates_to_new_generation() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'))
    manager_a = make_manager(kvs, status, groups=['RESIDENTIAL'], country_code='US')
    await manager_a.issue('sess1', None)

    status.queue('sess1~g2', status_ok('2.2.2.2'))
    manager_b = make_manager(kvs, status, groups=['DATACENTER'], country_code='US')
    lease, rotated = await manager_b.issue('sess1', None)

    assert rotated is True
    assert lease.generation == 2
    assert lease.groups == ['DATACENTER']
    assert lease.exit_id == '2.2.2.2'


async def test_quarantine_marks_lease_and_next_issue_rotates() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'))
    manager = make_manager(kvs, status)
    await manager.issue('sess1', None)

    await manager.quarantine_session('sess1')
    assert kvs.lease('sess1')['state'] == QUARANTINED_LEASE_STATE

    status.queue('sess1~g2', status_ok('3.3.3.3'))
    lease, rotated = await manager.issue('sess1', None)
    assert rotated is True
    assert lease.generation == 2
    assert lease.exit_id == '3.3.3.3'


async def test_foreign_process_cannot_quarantine_owned_lease() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('sess1', status_ok('1.1.1.1'))
    manager_a = make_manager(kvs, status)
    await manager_a.issue('sess1', None)

    manager_b = make_manager(kvs, status)
    with pytest.raises(ProxyLeaseOwnershipLostError):
        await manager_b.quarantine_session('sess1')


async def test_distinct_logical_sessions_have_independent_leases() -> None:
    kvs = InMemoryKvs()
    status = StatusScript()
    status.queue('a', status_ok('1.1.1.1'))
    status.queue('b', status_ok('2.2.2.2'))
    manager = make_manager(kvs, status)

    lease_a, _ = await manager.issue('a', None)
    lease_b, _ = await manager.issue('b', None)

    assert lease_a.physical_session_id == 'a'
    assert lease_b.physical_session_id == 'b'
    assert kvs.lease('a')['exit_id'] == '1.1.1.1'
    assert kvs.lease('b')['exit_id'] == '2.2.2.2'


async def test_handover_is_noop_without_any_lease() -> None:
    kvs = InMemoryKvs()
    manager = make_manager(kvs, StatusScript())

    await manager.handover()

    assert kvs.save_count == 0

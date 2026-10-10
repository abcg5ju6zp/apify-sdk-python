"""Persistable ownership leases for sticky Apify Proxy sessions.

When an Actor is migrated or rebooted, a sticky proxy session must not be resumed solely based on its session id:
the old process might still be renewing it, or the proxy exit behind that session might have silently changed.
This module binds each logical session to a lease that identifies the owning Actor run, the proxy tier/groups/
country identity and the process that currently owns it, and persists all leases to the run's default key-value
store so that they survive migration and reboot.
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from logging import getLogger
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import Protocol

logger = getLogger(__name__)

LEASE_CHECKPOINT_KEY = 'APIFY_PROXY_SESSION_LEASES'
LEASE_CHECKPOINT_VERSION = 1

ACTIVE_LEASE_STATE = 'active'
QUARANTINED_LEASE_STATE = 'quarantined'

SESSION_ID_MAX_LENGTH = 50

# Owner/generation are reconciled with the persisted checkpoint on every proxy issuance, while the proxy exit
# itself only needs to be re-verified through the proxy status endpoint within this interval.
EXIT_VERIFY_INTERVAL = timedelta(seconds=30)

# How many times the proxy status endpoint is queried before the exit is declared uncertain.
EXIT_STATUS_ATTEMPTS = 2


def _utc_now() -> datetime:
    """Return the current timezone-aware UTC time."""
    return datetime.now(UTC)


def _utc_now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return _utc_now().isoformat()


def build_physical_session_id(logical_session_id: str, generation: int) -> str:
    """Derive the proxy session id used in the proxy username for a lease generation.

    The first generation uses the logical session id as-is, so existing sessions keep their wire identity.
    Later generations get a deterministic, regex-safe suffix so that a rotation is observable while the logical
    session id stays stable for the caller.
    """
    if generation <= 1:
        return logical_session_id

    suffix = f'~g{generation}'
    if len(logical_session_id) + len(suffix) <= SESSION_ID_MAX_LENGTH:
        base = logical_session_id
    else:
        digest = hashlib.sha256(logical_session_id.encode()).hexdigest()[:8]
        base = f'{logical_session_id[: SESSION_ID_MAX_LENGTH - len(suffix) - len(digest) - 1]}~{digest}'

    physical_session_id = f'{base}{suffix}'
    return physical_session_id[:SESSION_ID_MAX_LENGTH]


def extract_exit_id(status: dict[str, Any]) -> str | None:
    """Extract the observed proxy exit identity from a proxy status response, if the endpoint provides one."""
    for key in ('clientIp', 'client_ip', 'exitIp', 'exit_ip', 'ip'):
        value = status.get(key)
        if isinstance(value, str) and value:
            return value
    return None


class ProxyLeaseError(Exception):
    """Base class for errors raised while acquiring or confirming a proxy session lease."""


class ProxyNotReadyError(ProxyLeaseError):
    """Raised when a leased proxy URL is requested before the proxy credentials are available.

    The request is deliberately not issued without a session; the caller is expected to retry once credential
    fetching succeeds.
    """


class ProxyExitUncertainError(ProxyLeaseError):
    """Raised when the proxy status cannot confirm that the original exit is still in use.

    The lease keeps pointing at the original exit and is re-verified on the next issuance; no rotation happens
    based on an uncertain result and no session-less fallback URL is ever returned.
    """

    def __init__(self, message: str, *, session_id: str, physical_session_id: str) -> None:
        super().__init__(message)
        self.session_id = session_id
        self.physical_session_id = physical_session_id


class ProxyLeaseOwnershipLostError(ProxyLeaseError):
    """Raised when another process has fenced this process out of a lease.

    The current process must not sign any more proxy URLs for the session.
    """


class ProxyLeasePersistError(ProxyLeaseError):
    """Raised when a lease checkpoint cannot be persisted.

    During migration or reboot this means the handover is rejected and the run resumes from the last successful
    checkpoint after restart.
    """


class _ExitVerification(StrEnum):
    """Outcome of verifying the actual exit behind a lease."""

    CONFIRMED = 'confirmed'
    INVALID = 'invalid'
    UNCERTAIN = 'uncertain'


@dataclass
class ProxyLease:
    """A persistable lease binding a logical proxy session to one confirmed proxy exit."""

    session_id: str
    """Logical session identifier as tracked by the caller. It never changes across rotations."""

    physical_session_id: str
    """Session identifier embedded in the proxy username. It changes whenever the exit is rotated."""

    generation: int
    """Monotonic generation, starting at 1. It is bumped on every exit rotation."""

    epoch: int
    """Monotonic ownership generation, starting at 1. It is bumped every time a different process takes the
    lease over; a process that observes a newer epoch than the one it owned is permanently fenced out."""

    owner_run_id: str | None
    """ID of the Actor run that owns the lease."""

    owner_token: str
    """Fencing token of the process instance that currently owns the lease."""

    proxy_tier: int | None
    """Proxy tier bound to the lease, if any."""

    groups: list[str]
    """Proxy groups bound to the lease."""

    country_code: str | None
    """Country code bound to the lease."""

    subdivision_code: str | None
    """Subdivision code bound to the lease."""

    username: str
    """Proxy authentication identity (encoded groups/session/country)."""

    exit_id: str | None
    """Observed identity of the actual proxy exit (e.g. its IP), if the status endpoint exposes it."""

    state: str = ACTIVE_LEASE_STATE
    """`active` or `quarantined`."""

    previous_exit_id: str | None = None
    """Observed exit identity of the previous generation after a rotation."""

    created_at: str = field(default_factory=_utc_now_iso)
    """Creation timestamp of the first generation, ISO-8601 in UTC."""

    last_confirmed_at: str | None = None
    """Timestamp of the last successful exit verification, ISO-8601 in UTC."""

    def identity_matches(
        self,
        *,
        proxy_tier: int | None,
        groups: list[str],
        country_code: str | None,
        subdivision_code: str | None,
    ) -> bool:
        """Check whether this lease is still valid for the requested proxy tier/groups/country identity."""
        return (
            self.proxy_tier == proxy_tier
            and self.groups == groups
            and self.country_code == country_code
            and self.subdivision_code == subdivision_code
        )

    def mark_confirmed(self, exit_id: str | None) -> None:
        """Record a successful exit verification."""
        if exit_id is not None:
            self.exit_id = exit_id
        self.last_confirmed_at = _utc_now_iso()

    def to_checkpoint_dict(self) -> dict[str, Any]:
        """Serialize the lease for the JSON checkpoint."""
        return {
            'session_id': self.session_id,
            'physical_session_id': self.physical_session_id,
            'generation': self.generation,
            'epoch': self.epoch,
            'owner_run_id': self.owner_run_id,
            'owner_token': self.owner_token,
            'proxy_tier': self.proxy_tier,
            'groups': list(self.groups),
            'country_code': self.country_code,
            'subdivision_code': self.subdivision_code,
            'username': self.username,
            'exit_id': self.exit_id,
            'state': self.state,
            'previous_exit_id': self.previous_exit_id,
            'created_at': self.created_at,
            'last_confirmed_at': self.last_confirmed_at,
        }

    @classmethod
    def from_checkpoint_dict(cls, data: dict[str, Any]) -> ProxyLease:
        """Deserialize a lease from the JSON checkpoint, tolerating missing optional fields."""
        return cls(
            session_id=str(data['session_id']),
            physical_session_id=str(data['physical_session_id']),
            generation=int(data['generation']),
            epoch=int(data.get('epoch', 1)),
            owner_run_id=data.get('owner_run_id'),
            owner_token=str(data['owner_token']),
            proxy_tier=data.get('proxy_tier'),
            groups=list(data.get('groups') or []),
            country_code=data.get('country_code'),
            subdivision_code=data.get('subdivision_code'),
            username=str(data.get('username') or ''),
            exit_id=data.get('exit_id'),
            state=str(data.get('state') or ACTIVE_LEASE_STATE),
            previous_exit_id=data.get('previous_exit_id'),
            created_at=str(data.get('created_at') or _utc_now_iso()),
            last_confirmed_at=data.get('last_confirmed_at'),
        )


if TYPE_CHECKING:

    class _LeaseKeyValueStore(Protocol):
        """Subset of the key-value store interface used by the lease checkpoint."""

        async def get_value(self, key: str) -> Any: ...

        async def set_value(self, key: str, value: Any) -> None: ...


class ProxyLeaseStore:
    """Loads and saves the lease checkpoint in a key-value store."""

    def __init__(self, kvs_opener: Callable[[], Awaitable[_LeaseKeyValueStore]]) -> None:
        self._kvs_opener = kvs_opener
        self._kvs: _LeaseKeyValueStore | None = None

    async def _get_kvs(self) -> _LeaseKeyValueStore:
        if self._kvs is None:
            self._kvs = await self._kvs_opener()
        return self._kvs

    async def load(self) -> dict[str, Any]:
        """Load the checkpoint document, returning an empty one if no valid checkpoint exists yet."""
        kvs = await self._get_kvs()
        checkpoint = await kvs.get_value(LEASE_CHECKPOINT_KEY)
        if not isinstance(checkpoint, dict) or checkpoint.get('version') != LEASE_CHECKPOINT_VERSION:
            return {'version': LEASE_CHECKPOINT_VERSION, 'owner_run_id': None, 'leases': {}}
        if not isinstance(checkpoint.get('leases'), dict):
            checkpoint['leases'] = {}
        return checkpoint

    async def save(self, checkpoint: dict[str, Any]) -> None:
        """Persist the checkpoint document, wrapping storage failures in `ProxyLeasePersistError`."""
        document = {
            **checkpoint,
            'version': LEASE_CHECKPOINT_VERSION,
            'updated_at': _utc_now_iso(),
        }
        try:
            kvs = await self._get_kvs()
            await kvs.set_value(LEASE_CHECKPOINT_KEY, document)
        except ProxyLeasePersistError:
            raise
        except Exception as exc:
            raise ProxyLeasePersistError(
                f'Failed to persist proxy session lease checkpoint under key {LEASE_CHECKPOINT_KEY!r}.'
            ) from exc


class ProxyLeaseManager:
    """Owns proxy session leases for one `ProxyConfiguration` instance/process."""

    def __init__(
        self,
        *,
        owner_run_id: str | None,
        groups: list[str],
        country_code: str | None,
        subdivision_code: str | None,
        store: ProxyLeaseStore,
        ensure_password: Callable[[], Awaitable[str]],
        build_proxy_url: Callable[[str, int | None], str],
        fetch_proxy_status: Callable[[str], Awaitable[dict[str, Any] | None]],
        verify_interval: timedelta = EXIT_VERIFY_INTERVAL,
    ) -> None:
        self._owner_run_id = owner_run_id
        self._groups = list(groups)
        self._country_code = country_code
        self._subdivision_code = subdivision_code
        self._store = store
        self._ensure_password_callback = ensure_password
        self._build_proxy_url = build_proxy_url
        self._fetch_proxy_status = fetch_proxy_status
        self._verify_interval = verify_interval

        # Unique per process: it fences out the previous process after a migration/reboot takeover.
        self._owner_token = uuid.uuid4().hex

        self._checkpoint: dict[str, Any] | None = None
        self._bootstrapped = False
        self._bootstrap_lock = asyncio.Lock()
        self._commit_lock = asyncio.Lock()
        self._session_locks: dict[str, asyncio.Lock] = {}
        # Epoch at which this process last owned each session lease. A higher epoch observed in the checkpoint
        # means this process has been fenced out and must never sign URLs for the session again.
        self._owned_epochs: dict[str, int] = {}

    async def issue(
        self,
        logical_session_id: str,
        proxy_tier: int | None,
        *,
        force_rotate: bool = False,
    ) -> tuple[ProxyLease, bool]:
        """Confirm ownership of a session lease and return the lease to sign proxy URLs with.

        Returns the active lease and whether the session was rotated (new generation/exit) during this call.
        Raises a `ProxyLeaseError` subclass when the proxy cannot be safely issued; a session-less URL is
        never returned as a fallback.
        """
        await self._bootstrap()

        lock = self._session_locks.setdefault(logical_session_id, asyncio.Lock())
        async with lock:
            password = await self._ensure_password_callback()
            if not password:
                raise ProxyNotReadyError(f'Apify Proxy password is not ready for proxy session {logical_session_id!r}.')

            await self._reconcile()

            existing = self._load_lease(logical_session_id)
            owned_epoch = self._owned_epochs.get(logical_session_id)
            rotated = False

            if existing is None:
                lease = self._new_lease(logical_session_id, generation=1, epoch=1, proxy_tier=proxy_tier)
                await self._claim(logical_session_id, lease)
            elif existing.owner_token == self._owner_token:
                must_rotate = (
                    force_rotate
                    or existing.state == QUARANTINED_LEASE_STATE
                    or existing.owner_run_id != self._owner_run_id
                    or not self._identity_matches(existing, proxy_tier)
                )
                if must_rotate:
                    lease = self._rotate_lease(existing, proxy_tier)
                    await self._claim(logical_session_id, lease)
                    rotated = True
                else:
                    # The reconcile above already confirmed that this process is still the owner at the same
                    # epoch, so no checkpoint write is needed before reusing the lease.
                    lease = existing
            elif owned_epoch is not None:
                raise ProxyLeaseOwnershipLostError(
                    f'Proxy session {logical_session_id!r} is now owned at epoch {existing.epoch} by another '
                    f'process; this process last owned epoch {owned_epoch} and cannot sign more proxy URLs for it.'
                )
            else:
                must_rotate = (
                    force_rotate
                    or existing.state == QUARANTINED_LEASE_STATE
                    or existing.owner_run_id != self._owner_run_id
                    or not self._identity_matches(existing, proxy_tier)
                )
                if must_rotate:
                    lease = self._rotate_lease(existing, proxy_tier)
                    rotated = True
                else:
                    lease = self._adopt_lease(existing, proxy_tier)
                await self._claim(logical_session_id, lease)

            verification = await self._verify_exit(lease)
            if verification == _ExitVerification.UNCERTAIN:
                raise ProxyExitUncertainError(
                    f'Proxy status for session {logical_session_id!r} is indeterminate; the original exit is '
                    'kept and will be re-verified on the next request.',
                    session_id=logical_session_id,
                    physical_session_id=lease.physical_session_id,
                )

            if verification != _ExitVerification.CONFIRMED:
                lease = await self._quarantine_and_rotate(logical_session_id, lease, proxy_tier)
                rotated = True

                follow_up = await self._verify_exit(lease)
                if follow_up == _ExitVerification.UNCERTAIN:
                    raise ProxyExitUncertainError(
                        f'Proxy status for the rotated session {logical_session_id!r} is indeterminate.',
                        session_id=logical_session_id,
                        physical_session_id=lease.physical_session_id,
                    )
                if follow_up != _ExitVerification.CONFIRMED:
                    raise ConnectionError(
                        f'Apify Proxy reported that the rotated exit for session {logical_session_id!r} is not '
                        'connected.'
                    )

            return lease, rotated

    async def quarantine_session(self, logical_session_id: str) -> None:
        """Quarantine a session lease so that the next issuance rotates to a new exit."""
        await self._bootstrap()
        lock = self._session_locks.setdefault(logical_session_id, asyncio.Lock())
        async with lock:
            await self._reconcile()
            lease = self._load_lease(logical_session_id)
            if lease is None or lease.state == QUARANTINED_LEASE_STATE:
                return
            if lease.owner_token != self._owner_token:
                raise ProxyLeaseOwnershipLostError(
                    f'Cannot quarantine proxy session {logical_session_id!r}: it is owned by another process.'
                )
            lease.state = QUARANTINED_LEASE_STATE
            self._store_lease(lease)
            await self._commit()

    async def handover(self) -> None:
        """Persist all known leases for migration/reboot.

        Raises `ProxyLeasePersistError` if the checkpoint cannot be saved, in which case the handover is
        rejected and the restart resumes from the previous checkpoint.
        """
        if not self._bootstrapped or self._checkpoint is None:
            return
        async with self._commit_lock:
            await self._store.save(self._checkpoint)

    async def _bootstrap(self) -> None:
        if self._bootstrapped:
            return
        async with self._bootstrap_lock:
            if self._bootstrapped:
                return
            self._checkpoint = await self._store.load()
            self._checkpoint.setdefault('owner_run_id', self._owner_run_id)
            self._bootstrapped = True

    async def _reconcile(self) -> None:
        """Re-read the checkpoint so that owner/generation are confirmed before every issuance."""
        self._checkpoint = await self._store.load()

    def _load_lease(self, logical_session_id: str) -> ProxyLease | None:
        assert self._checkpoint is not None  # noqa: S101
        raw_lease = self._checkpoint['leases'].get(logical_session_id)
        if not isinstance(raw_lease, dict):
            return None
        return ProxyLease.from_checkpoint_dict(raw_lease)

    def _store_lease(self, lease: ProxyLease) -> None:
        assert self._checkpoint is not None  # noqa: S101
        self._checkpoint['owner_run_id'] = self._owner_run_id
        self._checkpoint['leases'][lease.session_id] = lease.to_checkpoint_dict()

    async def _commit(self) -> None:
        assert self._checkpoint is not None  # noqa: S101
        async with self._commit_lock:
            await self._store.save(self._checkpoint)

    async def _claim(self, logical_session_id: str, lease: ProxyLease) -> ProxyLease:
        """Persist a new claim (ownership, adoption or rotation) and confirm it via fencing."""
        self._store_lease(lease)
        await self._commit()
        return await self._fence_or_lose(logical_session_id, lease)

    async def _fence_or_lose(self, logical_session_id: str, committed_lease: ProxyLease) -> ProxyLease:
        """Re-read the committed lease and lose ownership if another process won the claim."""
        assert self._checkpoint is not None  # noqa: S101
        self._checkpoint = await self._store.load()
        persisted = self._load_lease(logical_session_id)
        if (
            persisted is None
            or persisted.owner_token != self._owner_token
            or persisted.epoch != committed_lease.epoch
            or persisted.generation != committed_lease.generation
            or persisted.physical_session_id != committed_lease.physical_session_id
            or persisted.state != ACTIVE_LEASE_STATE
        ):
            raise ProxyLeaseOwnershipLostError(
                f'Ownership of proxy session {logical_session_id!r} generation {committed_lease.generation} was '
                'claimed by another process; no further proxy URLs can be signed for it from this process.'
            )
        self._owned_epochs[logical_session_id] = persisted.epoch
        return persisted

    def _identity_matches(self, lease: ProxyLease, proxy_tier: int | None) -> bool:
        """Check whether the lease matches this configuration's tier/groups/country identity."""
        return lease.identity_matches(
            proxy_tier=proxy_tier,
            groups=self._groups,
            country_code=self._country_code,
            subdivision_code=self._subdivision_code,
        )

    def _new_lease(
        self,
        logical_session_id: str,
        *,
        generation: int,
        epoch: int,
        proxy_tier: int | None,
    ) -> ProxyLease:
        physical_session_id = build_physical_session_id(logical_session_id, generation)
        return ProxyLease(
            session_id=logical_session_id,
            physical_session_id=physical_session_id,
            generation=generation,
            epoch=epoch,
            owner_run_id=self._owner_run_id,
            owner_token=self._owner_token,
            proxy_tier=proxy_tier,
            groups=list(self._groups),
            country_code=self._country_code,
            subdivision_code=self._subdivision_code,
            username=self._identity_username(physical_session_id),
            exit_id=None,
        )

    def _rotate_lease(self, previous: ProxyLease, proxy_tier: int | None) -> ProxyLease:
        """Isolate the previous lease and create the next generation for the same logical session."""
        generation = previous.generation + 1
        physical_session_id = build_physical_session_id(previous.session_id, generation)
        return ProxyLease(
            session_id=previous.session_id,
            physical_session_id=physical_session_id,
            generation=generation,
            epoch=previous.epoch + 1,
            owner_run_id=self._owner_run_id,
            owner_token=self._owner_token,
            proxy_tier=proxy_tier,
            groups=list(self._groups),
            country_code=self._country_code,
            subdivision_code=self._subdivision_code,
            username=self._identity_username(physical_session_id),
            exit_id=None,
            state=ACTIVE_LEASE_STATE,
            previous_exit_id=previous.exit_id,
            created_at=previous.created_at,
        )

    def _adopt_lease(self, previous: ProxyLease, proxy_tier: int | None) -> ProxyLease:
        """Take over an active lease in a new process, keeping its generation and original exit.

        The ownership epoch is bumped and the confirmation timestamp is reset so that the new process always
        verifies the original exit before signing the first URL, even if the previous process confirmed it
        shortly before the handover.
        """
        return ProxyLease(
            session_id=previous.session_id,
            physical_session_id=previous.physical_session_id,
            generation=previous.generation,
            epoch=previous.epoch + 1,
            owner_run_id=previous.owner_run_id,
            owner_token=self._owner_token,
            proxy_tier=proxy_tier,
            groups=list(previous.groups),
            country_code=previous.country_code,
            subdivision_code=previous.subdivision_code,
            username=previous.username,
            exit_id=previous.exit_id,
            state=ACTIVE_LEASE_STATE,
            previous_exit_id=previous.previous_exit_id,
            created_at=previous.created_at,
            last_confirmed_at=None,
        )

    async def _quarantine_and_rotate(
        self,
        logical_session_id: str,
        previous: ProxyLease,
        proxy_tier: int | None,
    ) -> ProxyLease:
        """Quarantine the old lease first, then acquire the next generation and fence-confirm it."""
        previous.state = QUARANTINED_LEASE_STATE
        self._store_lease(previous)
        await self._commit()

        new_lease = self._rotate_lease(previous, proxy_tier)
        return await self._claim(logical_session_id, new_lease)

    def _identity_username(self, physical_session_id: str) -> str:
        """Extract the authentication identity (username) from the built proxy URL."""
        url = self._build_proxy_url(physical_session_id, None)
        credentials = url.split('@', 1)[0]
        return credentials.removeprefix('http://').split(':', 1)[0]

    def _verification_due(self, lease: ProxyLease) -> bool:
        if lease.exit_id is None or lease.last_confirmed_at is None:
            return True
        try:
            last_confirmed = datetime.fromisoformat(lease.last_confirmed_at)
        except ValueError:
            return True
        return _utc_now() - last_confirmed >= self._verify_interval

    async def _verify_exit(self, lease: ProxyLease) -> _ExitVerification:
        """Query the proxy status through the leased exit and compare it with the recorded exit identity."""
        if not self._verification_due(lease):
            return _ExitVerification.CONFIRMED

        proxy_url = self._build_proxy_url(lease.physical_session_id, lease.proxy_tier)

        status: dict[str, Any] | None = None
        for _attempt in range(EXIT_STATUS_ATTEMPTS):
            status = await self._fetch_proxy_status(proxy_url)
            if status is not None:
                break

        if status is None:
            return _ExitVerification.UNCERTAIN

        if not status.get('connected'):
            return _ExitVerification.INVALID

        observed_exit_id = extract_exit_id(status)
        if lease.exit_id is not None and observed_exit_id is not None and observed_exit_id != lease.exit_id:
            logger.warning(
                'Proxy session %s switched exits unexpectedly: expected %s, observed %s.',
                lease.session_id,
                lease.exit_id,
                observed_exit_id,
            )
            return _ExitVerification.INVALID

        lease.mark_confirmed(observed_exit_id)
        self._store_lease(lease)
        # A failed freshness update must not block an otherwise confirmed request; strict persistence is
        # enforced for claims, rotations and migration handovers.
        try:
            await self._commit()
        except ProxyLeasePersistError:
            logger.warning('Could not refresh the lease confirmation timestamp for session %s.', lease.session_id)
        return _ExitVerification.CONFIRMED

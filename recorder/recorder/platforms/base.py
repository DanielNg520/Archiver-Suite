"""
recorder.platforms.base
────────────────────────
The LivePlatform Protocol. Structural typing (PEP 544): a class is a
LivePlatform if it has the right shape, with no need to inherit from
anything here.

Why Protocol, not ABC:
  A future TwitchLive in a separate package can satisfy this contract
  without importing recorder. The state machine depends on the Protocol;
  concrete platforms depend on nothing in this module. Dependency points
  inward toward the abstraction, never outward toward an implementation.

Contract:
  name         — short platform tag ("tiktok"), also the source/platform
                 value used downstream in the shared items table.
  is_live(u)   — cheap liveness check for one username. MUST be safe to
                 call on a 60s poll loop: no heavy connection, no
                 long-lived sockets. Returns False on any ambiguity or
                 transient error rather than raising — a poll loop should
                 never crash because one check timed out.
  stream_url(u)— resolve the current HLS URL for a live user. Only called
                 AFTER is_live(u) returned True. May raise; the caller
                 treats a raise as "couldn't start, back to listening".
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class LivePlatform(Protocol):
    name: str

    def is_live(self, username: str) -> bool: ...

    def stream_url(self, username: str) -> str: ...

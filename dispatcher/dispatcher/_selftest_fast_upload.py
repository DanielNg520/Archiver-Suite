"""
Self-test for dispatcher.fast_upload — the parallel multi-connection uploader.

Drives the real chunking/dispatch logic against a fake Telethon client that
records SaveBigFilePart requests, with zero network. Pins the contract the
send path relies on: byte-perfect reassembly, every part sent exactly once,
borrowed senders always returned, progress monotonic to 100%, and a hard
fallback to the serial uploader on any fast-path failure.

Run: PYTHONPATH=core:dispatcher python3 -m dispatcher._selftest_fast_upload
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from telethon.tl import types                                  # noqa: E402
from dispatcher import fast_upload                             # noqa: E402

_checks = 0


def check(cond: bool, label: str) -> None:
    global _checks
    _checks += 1
    if not cond:
        print(f"✗ FAIL: {label}")
        raise SystemExit(1)
    print(f"✓ {label}")


class _FakeSender:
    """Records each SaveBigFilePart it is asked to send."""
    def __init__(self, fail_on: int | None = None):
        self.parts: list[tuple[int, bytes]] = []
        self.fail_on = fail_on

    async def send(self, req):
        if self.fail_on is not None and req.file_part == self.fail_on:
            raise IOError("simulated transport failure")
        await asyncio.sleep(0)                # yield so workers truly interleave
        self.parts.append((req.file_part, bytes(req.bytes)))
        return True


class _FakeClient:
    def __init__(self, *, dc_id: int = 2, fail_on: int | None = None):
        self.session = SimpleNamespace(dc_id=dc_id)
        self._fail_on = fail_on
        self.borrowed: list[_FakeSender] = []
        self.returned: list[_FakeSender] = []
        self.serial_calls: list[str] = []

    async def _borrow_exported_sender(self, dc_id):
        s = _FakeSender(fail_on=self._fail_on)
        self.borrowed.append(s)
        return s

    async def _return_exported_sender(self, sender):
        self.returned.append(sender)

    async def upload_file(self, path, *, file_name=None, progress_callback=None):
        self.serial_calls.append(str(path))
        if progress_callback:
            progress_callback(os.path.getsize(path), os.path.getsize(path))
        return ("SERIAL", str(path))


def _make_file(path: Path, size: int) -> bytes:
    data = os.urandom(size)
    path.write_bytes(data)
    return data


def _reassemble(senders: list[_FakeSender]) -> bytes:
    parts = [p for s in senders for p in s.parts]
    return b"".join(chunk for _, chunk in sorted(parts, key=lambda x: x[0]))


def test_big_file_parallel(tmp: Path) -> None:
    print("\n── big file → parallel, byte-perfect, senders returned ──")
    size = 11 * 1024 * 1024 + 777          # >10 MiB, not part-aligned
    data = _make_file(tmp / "big.bin", size)
    client = _FakeClient()

    progress: list[tuple[int, int]] = []
    handle = asyncio.run(fast_upload.upload_file(
        client, tmp / "big.bin", connections=4,
        progress_callback=lambda c, t: progress.append((c, t))))

    check(not client.serial_calls, "the serial uploader was NOT used")
    check(isinstance(handle, types.InputFileBig), "returns an InputFileBig handle")
    expected_parts = (size + fast_upload.PART_SIZE - 1) // fast_upload.PART_SIZE
    check(handle.parts == expected_parts, "handle part_count matches the file")

    all_parts = [p for s in client.borrowed for p in s.parts]
    indices = sorted(i for i, _ in all_parts)
    check(indices == list(range(expected_parts)),
          "every part 0..N-1 sent exactly once (no gaps, no dupes)")
    check(_reassemble(client.borrowed) == data,
          "reassembled bytes are identical to the source file")
    check(len(client.borrowed) == 4
          and set(map(id, client.returned)) == set(map(id, client.borrowed)),
          "all 4 borrowed senders were returned (even on success)")
    check(progress and progress[-1] == (size, size),
          "progress callback ends at 100% (sent == total)")
    check([c for c, _ in progress] == sorted(c for c, _ in progress),
          "progress is monotonically non-decreasing")


def test_small_file_serial(tmp: Path) -> None:
    print("\n── small file → serial uploader (md5 path) ──")
    _make_file(tmp / "small.bin", 1024 * 1024)     # 1 MiB, under threshold
    client = _FakeClient()
    handle = asyncio.run(fast_upload.upload_file(
        client, tmp / "small.bin", connections=4))
    check(client.serial_calls == [str(tmp / "small.bin")],
          "delegated to client.upload_file")
    check(handle == ("SERIAL", str(tmp / "small.bin")),
          "returns the serial handle unchanged")
    check(not client.borrowed, "no senders borrowed for a small file")


def test_connections_one_is_serial(tmp: Path) -> None:
    print("\n── connections=1 → serial even for a big file ──")
    _make_file(tmp / "b.bin", 11 * 1024 * 1024)
    client = _FakeClient()
    asyncio.run(fast_upload.upload_file(client, tmp / "b.bin", connections=1))
    check(client.serial_calls and not client.borrowed,
          "connections=1 opts out of the parallel path")


def test_missing_internals_serial(tmp: Path) -> None:
    print("\n── missing Telethon internals → serial fallback ──")
    _make_file(tmp / "b.bin", 11 * 1024 * 1024)

    class _Bare:
        session = SimpleNamespace(dc_id=2)
        def __init__(self): self.serial_calls = []
        async def upload_file(self, path, *, file_name=None, progress_callback=None):
            self.serial_calls.append(str(path)); return ("SERIAL", str(path))

    client = _Bare()
    handle = asyncio.run(fast_upload.upload_file(
        client, tmp / "b.bin", connections=4))
    check(client.serial_calls and handle[0] == "SERIAL",
          "absent _borrow_exported_sender ⇒ serial path, no crash")


def test_part_failure_falls_back(tmp: Path) -> None:
    print("\n── a rejected part → fallback to serial, senders returned ──")
    _make_file(tmp / "b.bin", 11 * 1024 * 1024)
    client = _FakeClient(fail_on=3)            # sender raises on part 3
    handle = asyncio.run(fast_upload.upload_file(
        client, tmp / "b.bin", connections=4))
    check(client.serial_calls and handle[0] == "SERIAL",
          "parallel failure transparently falls back to the serial uploader")
    check(set(map(id, client.returned)) == set(map(id, client.borrowed)),
          "borrowed senders are returned even when a part fails mid-flight")


def main() -> int:
    print("dispatcher.fast_upload self-test")
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        test_big_file_parallel(tmp)
        test_small_file_serial(tmp)
        test_connections_one_is_serial(tmp)
        test_missing_internals_serial(tmp)
        test_part_failure_falls_back(tmp)
    print(f"\nALL PASS ({_checks} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

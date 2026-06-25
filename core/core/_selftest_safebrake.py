"""
Focused validation for the deletion-safebrake feature (DeletionGuard +
ProtectionPolicy) and the sent_items() query that backs `purge-sent`.

Run: python core/core/_selftest_safebrake.py

Standalone (no pytest). Builds a real ItemStore + PolicyStore on a temp DB and
a temp config.toml, exercises every guard decision path, and asserts the
dispatcher maybe_delete gate honors the safebrake.
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "core"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "dispatcher"))

from core import (                                    # noqa: E402
    ItemStore, PolicyStore, DeletionGuard, ProtectionPolicy,
    DeletePolicy, RecorderDeletePolicy, Status,
)

OK = "✓"
_checks = 0


def check(cond: bool, label: str) -> None:
    global _checks
    if not cond:
        raise AssertionError(f"FAILED: {label}")
    _checks += 1
    print(f"{OK} {label}")


def _touch(d: Path, name: str) -> str:
    p = d / name
    p.write_bytes(b"x" * 1024)
    return str(p)


def main() -> int:
    tmp = Path(tempfile.mkdtemp())
    db = ItemStore.open(str(tmp / "suite.db"))
    policy = PolicyStore(tmp / "config.toml")
    guard = DeletionGuard(policy)

    # ── ProtectionPolicy resolution (user → platform → global) ──────────────
    prot = ProtectionPolicy(policy)
    check(prot.is_protected("x", "alice") is False, "protection default OFF")

    policy.set(ProtectionPolicy.KEY, True, platform="x", username="alice")
    check(prot.is_protected("x", "alice") is True, "per-user protection set")
    check(prot.is_protected("x", "bob") is False, "sibling user unaffected")

    policy.set(ProtectionPolicy.KEY, True, platform="tiktok")
    check(prot.is_protected("tiktok", "anyone") is True, "platform-wide protection")
    check(prot.is_protected("instagram", "anyone") is False, "other platform free")

    # ── DeletionGuard.delete: skips protected, deletes unprotected ──────────
    prot_file = _touch(tmp, "protected.mp4")
    free_file = _touch(tmp, "free.mp4")
    kept = guard.delete("x", "alice", prot_file, reason="test")
    check(kept is False and Path(prot_file).exists(),
          "guard keeps protected file (returns False)")
    removed = guard.delete("instagram", "bob", free_file, reason="test")
    check(removed is True and not Path(free_file).exists(),
          "guard deletes unprotected file (returns False→True)")

    # sidecar removed alongside the main file
    main_f = _touch(tmp, "clip.mp4")
    side = tmp / "clip.json"
    side.write_text("{}")
    guard.delete("instagram", "bob", main_f, reason="test")
    check(not Path(main_f).exists() and not side.exists(),
          "guard removes sidecars too")

    # ── dispatcher maybe_delete honors the safebrake ────────────────────────
    from dispatcher.delete import maybe_delete  # noqa: E402

    del_pol = DeletePolicy(policy)
    rec_pol = RecorderDeletePolicy(policy)
    # delete-after-upload ON globally so only the safebrake can keep a file
    policy.set(DeletePolicy.KEY, True)

    def _add_sent(source, platform, username, identifier, path):
        """Insert a row and drive it pending→sending→sent (the only legal
        route to 'sent'). One pending at a time, so claim_next() returns it."""
        db.add_item(source=source, platform=platform, username=username,
                    identifier=identifier, file_path=path)
        claimed = db.claim_next()
        assert claimed is not None and claimed.file_path == path, "claim race"
        db.mark_sent(claimed.id)
        return claimed.id

    # protected archiver row (x/alice) → kept despite delete_after_upload=true
    pf = _touch(tmp, "alice_post.mp4")
    aid = _add_sent("archiver", "x", "alice", "post_alice_1", pf)
    maybe_delete(db, aid, delete_policy=del_pol,
                 recorder_delete_policy=rec_pol, guard=guard)
    check(Path(pf).exists(), "maybe_delete keeps safebraked archiver file")

    # unprotected archiver row (instagram/bob) → deleted
    bf = _touch(tmp, "bob_post.mp4")
    bid = _add_sent("archiver", "instagram", "bob", "post_bob_1", bf)
    maybe_delete(db, bid, delete_policy=del_pol,
                 recorder_delete_policy=rec_pol, guard=guard)
    check(not Path(bf).exists(), "maybe_delete deletes unprotected archiver file")

    # recorder row under protected platform tiktok → kept
    rf = _touch(tmp, "rekta_live.mp4")
    rid = _add_sent("recorder", "tiktok", "rekta02", "recorder_rekta_live", rf)
    maybe_delete(db, rid, delete_policy=del_pol,
                 recorder_delete_policy=rec_pol, guard=guard)
    check(Path(rf).exists(), "maybe_delete keeps safebraked recorder file")

    # ── sent_items() query (backs purge-sent) ───────────────────────────────
    sent_all = db.sent_items()
    check(len(sent_all) == 3, "sent_items() returns all sent rows")
    check(all(it.status == Status.SENT.value for it in sent_all),
          "sent_items() rows are all 'sent'")
    check(len(db.sent_items(platform="instagram")) == 1,
          "sent_items(platform=) filters")
    check(len(db.sent_items(source="recorder")) == 1,
          "sent_items(source=) filters")
    check(len(db.sent_items(platform="x", username="alice")) == 1,
          "sent_items(platform+user) filters")
    check(db.sent_items(platform="nope") == [],
          "sent_items() empty for unknown scope")

    # ── orphaned (chat_id folder) ship-and-delete: file + row, no trace ──────
    # A chat_id folder is a drop-zone, not an archive: an uploaded orphaned item
    # deletes BOTH its file and its row, UNCONDITIONALLY — independent of the
    # platform-archive delete_after_upload policy. Turn that policy OFF to prove
    # the orphaned path doesn't depend on it.
    policy.unset(DeletePolicy.KEY)
    check(del_pol.should_delete("orphaned", "-100777") is False,
          "precondition: delete_after_upload is OFF")
    of = _touch(tmp, "orphan_drop.mp4")
    oid = _add_sent("orphaned", "orphaned", "-100777", "orphaned_drop_1", of)
    maybe_delete(db, oid, delete_policy=del_pol,
                 recorder_delete_policy=rec_pol, guard=guard)
    check(not Path(of).exists(),
          "orphaned file removed despite delete_after_upload OFF")
    check(db.get(oid) is None,
          "orphaned ROW removed too — chat_id folder leaves no trace")

    # Safebrake coupling: a PROTECTED orphaned scope keeps BOTH file and row.
    # Deleting the row while the file remains would let the ingester re-upload
    # it, so row-deletion is coupled to the file actually being gone.
    policy.set(ProtectionPolicy.KEY, True, platform="orphaned", username="-100888")
    pof = _touch(tmp, "orphan_protected.mp4")
    poid = _add_sent("orphaned", "orphaned", "-100888", "orphaned_prot_1", pof)
    maybe_delete(db, poid, delete_policy=del_pol,
                 recorder_delete_policy=rec_pol, guard=guard)
    check(Path(pof).exists(), "safebraked orphaned file KEPT")
    check(db.get(poid) is not None,
          "safebraked orphaned ROW KEPT too (file present ⇒ row stays as the "
          "re-ingest guard)")

    # ── unset restores deletability ─────────────────────────────────────────
    policy.unset(ProtectionPolicy.KEY, platform="x", username="alice")
    check(prot.is_protected("x", "alice") is False, "unset clears protection")
    check(guard.delete("x", "alice", pf, reason="test") is True
          and not Path(pf).exists(),
          "guard deletes after safebrake removed")

    db.close()
    print(f"\nALL PASS ({_checks} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

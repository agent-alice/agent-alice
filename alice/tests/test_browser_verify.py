"""Synthetic process snapshots for verification cleanup, without signalling PIDs."""

from alice_codex.browser_verify import _remember_owned, _remaining_owned


def test_root_pid_reuse_cannot_capture_or_signal_the_replacement_tree():
    owned = {}
    original = {
        100: (1, 100, "S", "original root"),
        101: (100, 101, "S", "original child"),
    }
    _remember_owned(original, 100, "original root", owned)
    replacement = {
        100: (1, 100, "S", "unrelated replacement root"),
        101: (1, 101, "S", "original child"),
        102: (100, 100, "S", "unrelated replacement child"),
    }
    _remember_owned(replacement, 100, "original root", owned)
    assert owned == {100: "original root", 101: "original child"}
    assert _remaining_owned(replacement, owned) == [101]


def test_child_pid_reuse_keeps_original_birth_and_does_not_expand_its_tree():
    owned = {100: "root", 101: "original child"}
    snapshot = {
        100: (1, 100, "S", "root"),
        101: (100, 101, "S", "replacement child"),
        102: (101, 101, "S", "replacement descendant"),
        103: (100, 103, "S", "other owned child"),
    }
    _remember_owned(snapshot, 100, "root", owned)
    assert owned == {100: "root", 101: "original child", 103: "other owned child"}
    assert _remaining_owned(snapshot, owned) == [100, 103]


def test_exited_root_allows_cleanup_of_previously_recorded_descendants_only():
    owned = {100: "root", 101: "child", 102: "exited child"}
    snapshot = {
        101: (1, 101, "S", "child"),
        102: (1, 102, "Z", "exited child"),
        103: (1, 100, "S", "unrecorded process"),
    }
    _remember_owned(snapshot, 100, "root", owned)
    assert owned == {100: "root", 101: "child", 102: "exited child"}
    assert _remaining_owned(snapshot, owned) == [101]


def test_no_verified_root_birth_never_establishes_ownership():
    snapshot = {100: (1, 100, "S", "unknown root"), 101: (100, 100, "S", "unknown child")}
    owned = {}
    _remember_owned(snapshot, 100, None, owned)
    assert owned == {}


def test_original_live_root_captures_descendants_and_process_group_members():
    snapshot = {
        100: (1, 100, "S", "root"),
        101: (100, 101, "S", "child"),
        102: (101, 102, "S", "grandchild"),
        103: (1, 100, "S", "group member"),
        104: (1, 104, "S", "unrelated"),
    }
    owned = {}
    _remember_owned(snapshot, 100, "root", owned)
    assert owned == {100: "root", 101: "child", 102: "grandchild", 103: "group member"}

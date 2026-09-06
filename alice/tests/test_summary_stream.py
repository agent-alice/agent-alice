"""Pure streaming-source tests use only generated, isolated byte fixtures."""

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import random
import tracemalloc

import pytest

from alice_codex import summary_stream as stream


def source(tmp_path, raw, name="source.jsonl"):
    path = tmp_path / name
    path.write_bytes(raw)
    return path


@pytest.mark.parametrize("read_bytes", [1, 2, 7, 65536])
def test_top_level_times_survive_utf8_escapes_and_crlf_across_blocks(tmp_path, monkeypatch, read_bytes):
    monkeypatch.setattr(stream, "READ_BYTES", read_bytes)
    # json.dumps makes escaping unambiguous while exercising all JSON escapes.
    body = {
        "text": '中文🙂 "timestamp": "not metadata"\b\f\n\r\t\\/',
        "nested": {"timestamp": "nested-only", "time_start": "not top-level"},
        "timestamp": "2026-09-01T00:30:00+08:00",
        "time_start": "2026-09-01T00:00:00+08:00",
    }
    first = json.dumps(body, ensure_ascii=False).replace(
        '"timestamp": "2026-', '"time\\u0073tamp": "2026-'
    ).encode() + b"\r\n"
    second = b'{"content":"last record without a newline"}'
    path = source(tmp_path, first + second)

    records = list(stream.scan_records(path))

    assert records == [
        stream.RecordSpan(1, 0, len(first), {
            "timestamp": body["timestamp"], "time_start": body["time_start"],
        }),
        stream.RecordSpan(2, len(first), len(first + second)),
    ]


@pytest.mark.parametrize("read_bytes", [1, 11, 65536])
def test_complete_grammar_accepts_generated_json_without_materializing_values(tmp_path, monkeypatch, read_bytes):
    monkeypatch.setattr(stream, "READ_BYTES", read_bytes)
    rng = random.Random(148)
    values = [None, True, False, 0, -0.25, 1e100, "", "\\\"中文🙂", {}, []]
    for index in range(80):
        values.append({
            "index": index,
            "payload": [rng.choice(values[:10]), {"nested": rng.choice(values[:10])}],
            "timestamp": "nested time is ordinary content",
        })
    raw = b"".join(json.dumps({"value": value}, ensure_ascii=bool(index % 2)).encode() + b"\n"
                   for index, value in enumerate(values))
    records = list(stream.scan_records(source(tmp_path, raw)))
    assert len(records) == len(values)
    assert all(record.parse_error is None and record.metadata == {} for record in records)
    assert records[0].byte_start == 0 and records[-1].byte_end == len(raw)
    assert all(left.byte_end == right.byte_start for left, right in zip(records, records[1:]))


@pytest.mark.parametrize("raw", [
    b'{"x":01}', b'{"x":-}', b'{"x":1.}', b'{"x":1e+}',
    b'{"x":true false}', b'{"x":"\\q"}', b'{"x":"\\u12xz"}',
    b'{"x":"raw\tcontrol"}', b'{"x":[]}{}', b'{"x":[1,]}', b'{"x":1,}',
    b'{"x" 1}', b'{"x":NaN}', b'{"x":Infinity}', b'{"x":null',
    b'{"x":"unterminated}', b'{}\x0c', b'\xef\xbb\xbf{}', b'',
])
def test_malformed_json_is_drained_and_does_not_hide_next_record(tmp_path, raw):
    tail = b'{"timestamp":"2026-09-01T00:00:00Z"}\n'
    path = source(tmp_path, raw + b"\n" + tail)
    records = list(stream.scan_records(path))
    assert records[0] == stream.RecordSpan(1, 0, len(raw) + 1, {}, "invalid_json")
    assert records[1] == stream.RecordSpan(
        2, len(raw) + 1, len(raw) + 1 + len(tail), {"timestamp": "2026-09-01T00:00:00Z"}
    )


@pytest.mark.parametrize("raw", [b"null", b"true", b"false", b"1.2e-3", b'"text"', b"[]"])
def test_valid_non_object_json_has_an_explicit_type_error(tmp_path, raw):
    assert list(stream.scan_records(source(tmp_path, raw))) == [
        stream.RecordSpan(1, 0, len(raw), {}, "json_not_object")
    ]


@pytest.mark.parametrize("key", ["timestamp", "time_start"])
def test_duplicate_top_level_time_key_is_rejected_even_when_escaped_or_equal(tmp_path, key):
    escaped_key = key.replace("t", "\\u0074", 1)
    raw = ('{"' + key + '":"same","' + escaped_key + '":"same"}').encode()
    record, = stream.scan_records(source(tmp_path, raw))
    assert record.metadata == {} and record.parse_error == "duplicate_time_key"
    assert record.byte_end == len(raw)


@pytest.mark.parametrize("value", [None, 42, True, {}, []])
def test_non_string_top_level_time_metadata_is_explicitly_invalid(tmp_path, value):
    raw = json.dumps({"timestamp": value}).encode()
    record, = stream.scan_records(source(tmp_path, raw))
    assert record.metadata == {} and record.parse_error == "invalid_time_metadata"


def test_time_metadata_limit_does_not_limit_other_object_keys_or_values(tmp_path):
    raw = json.dumps({
        "timestamp": "x" * stream.MAX_METADATA_CHARS,
        "a" * 4096: "v" * 4096,
    }).encode()
    valid, = stream.scan_records(source(tmp_path, raw))
    assert valid.metadata == {"timestamp": "x" * stream.MAX_METADATA_CHARS}
    assert valid.parse_error is None
    over = json.dumps({"timestamp": "x" * (stream.MAX_METADATA_CHARS + 1)}).encode()
    invalid, = stream.scan_records(source(tmp_path, over))
    assert invalid.metadata == {} and invalid.parse_error == "time_metadata_exceeds_limit"


@pytest.mark.parametrize("read_bytes", [1, 5, 65536])
def test_selected_metadata_decodes_surrogate_pairs_before_its_character_limit(tmp_path, monkeypatch, read_bytes):
    monkeypatch.setattr(stream, "READ_BYTES", read_bytes)
    value = "🙂" * stream.MAX_METADATA_CHARS
    raw = json.dumps({"timestamp": value}).encode()
    record, = stream.scan_records(source(tmp_path, raw))
    assert record.metadata == {"timestamp": value} and record.parse_error is None
    for invalid in (b'{"timestamp":"\\ud800"}', b'{"time_start":"\\udfff"}'):
        record, = stream.scan_records(source(tmp_path, invalid))
        assert record.metadata == {} and record.parse_error == "invalid_time_metadata"


class _ObjectPairs(list):
    """Keep stdlib-decoded object keys, including duplicates, distinct from arrays."""


def reference_metadata(raw):
    """Independent oracle: stdlib JSON grammar plus this helper's metadata rules."""
    def no_constant(value):
        raise ValueError("Non-JSON constant: " + value)

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {}, "invalid_utf8"
    try:
        value = json.loads(text, object_pairs_hook=_ObjectPairs, parse_constant=no_constant)
    except (ValueError, RecursionError):
        return {}, "invalid_json"
    if not isinstance(value, _ObjectPairs):
        return {}, "json_not_object"
    result = {}
    for key, content in value:
        if key not in {"timestamp", "time_start"}:
            continue
        if key in result:
            return {}, "duplicate_time_key"
        if not isinstance(content, str):
            return {}, "invalid_time_metadata"
        if len(content) > stream.MAX_METADATA_CHARS:
            return {}, "time_metadata_exceeds_limit"
        try:
            content.encode("utf-8")
        except UnicodeEncodeError:
            return {}, "invalid_time_metadata"
        result[key] = content
    return result, None


@pytest.mark.parametrize("read_bytes", [1, 17, 65536])
def test_fixed_seed_insert_delete_replace_and_truncate_variants_match_stdlib_oracle(
    tmp_path, monkeypatch, read_bytes, record_property
):
    monkeypatch.setattr(stream, "READ_BYTES", read_bytes)
    # Include syntax not emitted by json.dumps, then mutate lexical/structural
    # boundaries. Expected validity comes from an independent parser, not the
    # source generator or this scanner's implementation.
    bases = [
        b'{"n":-0,"f":0.25E+07,"ok":true,"nil":null}',
        b'{ "nested" : [false,{"s":"\\u0041\\/\\t\\b\\f\\r\\n"}],"timestamp":"t" }',
        b'{"time\\u005fstart":"2026-09-01T00:00:00Z","timestamp":"2026-09-01T00:30:00Z"}',
        b'{"a":1,"a":2,"nested":{"timestamp":"n","timestamp":"n2"}}',
        b'{"timestamp":"same","time\\u0073tamp":"same"}',
        b'{"timestamp":"\\ud83d\\ude42","time_start":"\\ud800"}',
        '{"文":"中文🙂","x":[[[],{}]]}'.encode(),
    ]
    rng = random.Random(908177)
    variants = list(bases)
    alphabet = b'{}[],:"\\0123456789.-+eEtruefalsn \txyz'
    for _ in range(360):
        raw = rng.choice(bases)
        at = rng.randrange(len(raw) + 1)
        operation = rng.randrange(4)
        if operation == 0:
            changed = raw[:at] + bytes([rng.choice(alphabet)]) + raw[at:]
        elif operation == 1:
            changed = raw[:at] + raw[at + 1:]
        elif operation == 2:
            changed = raw[:at] + bytes([rng.choice(alphabet)]) + raw[at + 1:]
        else:
            changed = raw[:at]
        variants.append(changed)
    raw_file = b"\n".join(variants) + b"\n"
    records = list(stream.scan_records(source(tmp_path, raw_file)))
    assert len(records) == len(variants)
    valid_count = invalid_count = 0
    for raw, record in zip(variants, records):
        metadata, error = reference_metadata(raw)
        if error is None:
            valid_count += 1
            assert record.parse_error is None and record.metadata == metadata, raw
        else:
            invalid_count += 1
            # A record can violate multiple rules. It must be invalid, but the
            # first streaming error need not equal a whole-document parser's.
            assert record.parse_error is not None and record.metadata == {}, raw
        assert record.byte_end - record.byte_start == len(raw) + 1
    assert valid_count >= 10 and invalid_count >= 100
    record_property("stdlib_oracle_valid_variants", valid_count)
    record_property("stdlib_oracle_invalid_variants", invalid_count)


def test_nesting_bound_is_explicit_and_next_line_is_still_available(tmp_path):
    def nested(depth):
        return b'{"nested":' + b"[" * depth + b"0" + b"]" * depth + b"}\n"

    raw = nested(stream.MAX_JSON_DEPTH - 1) + nested(stream.MAX_JSON_DEPTH) + b"{}\n"
    records = list(stream.scan_records(source(tmp_path, raw)))
    assert [record.parse_error for record in records] == [None, "json_depth_limit", None]
    assert records[-1].byte_end == len(raw)


@pytest.mark.parametrize("bad", [b"\xff", b"\xc3", b"\xed\xa0\x80", b"\xf0\x80\x80\x80"])
def test_bad_utf8_is_explicit_and_never_produces_trusted_metadata(tmp_path, monkeypatch, bad):
    monkeypatch.setattr(stream, "READ_BYTES", 1)
    first = b'{"timestamp":"2026-09-01T00:00:00Z","content":"' + bad + b'"}\n'
    raw = first + b"{}\n"
    records = list(stream.scan_records(source(tmp_path, raw)))
    assert records[0] == stream.RecordSpan(1, 0, len(first), {}, "invalid_utf8")
    assert records[1] == stream.RecordSpan(2, len(first), len(raw))


@pytest.mark.parametrize("suffix", [".md", ".markdown"])
def test_markdown_is_one_byte_exact_document_including_empty_files(tmp_path, suffix):
    path = source(tmp_path, b"one\r\ntwo\n{broken}\rthree", "note" + suffix)
    assert list(stream.scan_records(path)) == [stream.RecordSpan(0, 0, path.stat().st_size)]
    path.write_bytes(b"")
    assert list(stream.scan_records(path)) == [stream.RecordSpan(0, 0, 0)]
    path.write_bytes(b"invalid \xff text")
    record, = stream.scan_records(path)
    assert record.parse_error == "invalid_utf8"


def test_plain_text_lines_are_not_mistaken_for_json_and_empty_jsonl_has_no_records(tmp_path):
    path = source(tmp_path, b"ordinary\r\ntext", "file.log")
    assert list(stream.scan_records(path)) == [
        stream.RecordSpan(1, 0, 10), stream.RecordSpan(2, 10, 14),
    ]
    assert list(stream.scan_records(source(tmp_path, b""))) == []


@pytest.mark.parametrize("max_bytes", [4, 5, 7, 13, 65536])
def test_fragments_preserve_utf8_original_bytes_hashes_and_exact_coverage(tmp_path, max_bytes):
    raw = ("a中文🙂𐀀\r\n" * 19).encode()
    path = source(tmp_path, raw, "unicode.md")
    fragments = list(stream.fragment_ranges(path, 0, len(raw), max_bytes=max_bytes))
    assert fragments[0].byte_start == 0 and fragments[-1].byte_end == len(raw)
    assert all(left.byte_end == right.byte_start for left, right in zip(fragments, fragments[1:]))
    reassembled = []
    for fragment in fragments:
        body = raw[fragment.byte_start:fragment.byte_end]
        assert 0 < len(body) <= max_bytes
        assert fragment.sha256 == hashlib.sha256(body).hexdigest()
        assert fragment.parse_error is None
        body.decode("utf-8")
        reassembled.append(body)
    assert b"".join(reassembled) == raw


def test_invalid_utf8_fragments_remain_addressable_and_hash_every_byte(tmp_path):
    raw = b"head" + b"\xff\xc3\xf0\x80\x80\x80" + "中文🙂".encode() + b"tail"
    path = source(tmp_path, raw, "invalid.md")
    fragments = list(stream.fragment_ranges(path, 4, len(raw) - 4, max_bytes=5))
    assert any(fragment.parse_error == "invalid_utf8" for fragment in fragments)
    assert fragments[0].byte_start == 4 and fragments[-1].byte_end == len(raw) - 4
    assert all(left.byte_end == right.byte_start for left, right in zip(fragments, fragments[1:]))
    bodies = [raw[fragment.byte_start:fragment.byte_end] for fragment in fragments]
    assert b"".join(bodies) == raw[4:-4]
    assert [fragment.sha256 for fragment in fragments] == [hashlib.sha256(body).hexdigest() for body in bodies]


@pytest.mark.parametrize("start,end,limit", [(-1, 1, 4), (2, 1, 4), (0, 1, 3), (True, 1, 4), (0, 99, 4)])
def test_fragment_range_errors_do_not_return_partial_success(tmp_path, start, end, limit):
    path = source(tmp_path, b"four", "data.md")
    with pytest.raises(ValueError):
        list(stream.fragment_ranges(path, start, end, max_bytes=limit))
    assert list(stream.fragment_ranges(path, 2, 2)) == []


def test_giant_body_before_timestamp_and_giant_fragment_keep_bounded_reads_and_heap(
    tmp_path, monkeypatch, record_property
):
    path = tmp_path / "giant.jsonl"
    with path.open("wb") as handle:
        handle.write(b'{"content":"')
        for _ in range(256):
            handle.write(b"x" * 65536)
        handle.write(b'","nested":{"timestamp":"not top-level"},"timestamp":"2026-09-01T00:30:00Z"}\r\n')
    expected_size = path.stat().st_size
    original_open = Path.open
    requested_sizes = []

    @contextmanager
    def bounded_open(current, mode="r", *args, **kwargs):
        with original_open(current, mode, *args, **kwargs) as handle:
            if current != path or mode != "rb":
                yield handle
                return

            class Reader:
                def read(self, size=-1):
                    assert 0 <= size <= stream.READ_BYTES
                    requested_sizes.append(size)
                    return handle.read(size)

                def seek(self, *args):
                    return handle.seek(*args)

                def tell(self):
                    return handle.tell()

            yield Reader()

    monkeypatch.setattr(Path, "open", bounded_open)
    tracemalloc.start()
    try:
        records = list(stream.scan_records(path))
        fragments = list(stream.fragment_ranges(path, 0, expected_size, max_bytes=expected_size))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert records == [stream.RecordSpan(1, 0, expected_size, {"timestamp": "2026-09-01T00:30:00Z"})]
    assert len(fragments) == 1 and fragments[0].parse_error is None
    with original_open(path, "rb") as handle:
        assert fragments[0].sha256 == hashlib.file_digest(handle, "sha256").hexdigest()
    assert peak < 2 * 1024 * 1024, f"Streaming Python allocation exceeded 2 MiB: {peak}"
    assert requested_sizes and max(requested_sizes) <= stream.READ_BYTES
    record_property("stream_peak_python_bytes", peak)
    record_property("stream_source_bytes", expected_size)

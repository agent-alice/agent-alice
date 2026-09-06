"""Read frozen summary sources without buffering whole records or JSON strings.

These helpers do not write files or create source IDs. The caller owns freezing
and version checks. Offsets and hashes always describe original bytes, including
line endings; decoded text is used only to validate UTF-8 and JSON metadata.
"""

import codecs
from dataclasses import dataclass, field
import hashlib
from pathlib import Path
import re
from typing import BinaryIO, Iterator


READ_BYTES = 65536
MAX_JSON_DEPTH = 128
MAX_METADATA_CHARS = 256
_TIME_KEYS = frozenset({"timestamp", "time_start"})
_STRING_SPECIAL = re.compile(r'["\\\x00-\x1f]')
_ESCAPES = {'"': '"', "\\": "\\", "/": "/", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t"}
_WHITESPACE = frozenset(" \t\r\n")
_HEX = frozenset("0123456789abcdefABCDEF")
_LINE_END = re.compile(b"[\r\n]")


@dataclass(frozen=True)
class RecordSpan:
    line: int
    byte_start: int
    byte_end: int
    metadata: dict[str, str] = field(default_factory=dict)
    parse_error: str | None = None


@dataclass(frozen=True)
class FragmentSpan:
    byte_start: int
    byte_end: int
    sha256: str
    parse_error: str | None = None


@dataclass
class _Frame:
    kind: str
    state: str
    key: str | None = None


class _JsonScanner:
    """A bounded JSON grammar recognizer, not a prefix or regex extractor.

    All JSON tokens are checked. Only object keys and the two selected top-level
    strings can be retained; arbitrary strings and numbers are consumed without
    constructing their values. The regular expression only locates the next
    escape, quote or control character *inside an already recognized string*.
    """

    def __init__(self):
        self.stack: list[_Frame] = []
        self.started = False
        self.object_root = False
        self.error: str | None = None
        self.metadata: dict[str, str] = {}
        self.seen_times: set[str] = set()
        self.mode: str | None = None
        self.string_state = "normal"
        self.string_key = False
        self.string_target: str | None = None
        self.string_parts: list[str] = []
        self.string_size = 0
        self.capture = False
        self.pending_surrogate: int | None = None
        self.unicode_digits = ""
        self.number_state = ""
        self.literal = ""
        self.literal_at = 0

    def _fail(self, reason="invalid_json"):
        self.error = self.error or reason

    def _string_start(self, *, key=False, target=None):
        self.mode = "string"
        self.string_state = "normal"
        self.string_key, self.string_target = key, target
        self.string_parts, self.string_size = [], 0
        self.capture = key or target is not None
        self.pending_surrogate = None

    def _string_add(self, text):
        if not self.capture:
            return
        if self.pending_surrogate is not None:
            if text and 0xDC00 <= ord(text[0]) <= 0xDFFF:
                codepoint = 0x10000 + ((self.pending_surrogate - 0xD800) << 10) + ord(text[0]) - 0xDC00
                text = chr(codepoint) + text[1:]
            else:
                text = chr(self.pending_surrogate) + text
            self.pending_surrogate = None
        if text and 0xD800 <= ord(text[-1]) <= 0xDBFF:
            self.pending_surrogate = ord(text[-1])
            text = text[:-1]
        self._string_store(text)

    def _string_store(self, text):
        self.string_size += len(text)
        limit = max(map(len, _TIME_KEYS)) if self.string_key else MAX_METADATA_CHARS
        if self.string_size > limit:
            # A long key cannot name either selected field. Its syntax is still
            # validated, but retaining the key would make memory input-sized.
            self.capture = False
            self.string_parts = []
            if not self.string_key:
                self._fail("time_metadata_exceeds_limit")
        else:
            self.string_parts.append(text)

    def _string_end(self):
        if self.capture and self.pending_surrogate is not None:
            self._string_store(chr(self.pending_surrogate))
            self.pending_surrogate = None
        value = "".join(self.string_parts) if self.capture else None
        if self.string_key:
            frame = self.stack[-1]
            frame.key, frame.state = value, "colon"
            if len(self.stack) == 1 and value in _TIME_KEYS:
                if value in self.seen_times:
                    self._fail("duplicate_time_key")
                self.seen_times.add(value)
        elif self.string_target is not None:
            if value is not None:
                try:
                    value.encode("utf-8")
                except UnicodeEncodeError:
                    # JSON permits escaped unpaired surrogates, but a time
                    # string retained as UTF-8 metadata must be representable.
                    self._fail("invalid_time_metadata")
                else:
                    self.metadata[self.string_target] = value
        self.mode = None
        self.string_parts = []

    def _string_feed(self, text, index):
        if self.string_state == "normal":
            match = _STRING_SPECIAL.search(text, index)
            end = match.start() if match else len(text)
            self._string_add(text[index:end])
            if self.error or match is None:
                return end
            char = text[end]
            if char == '"':
                self._string_end()
            elif char == "\\":
                self.string_state = "escape"
            else:
                self._fail()
            return end + 1
        char = text[index]
        if self.string_state == "escape":
            if char == "u":
                self.string_state, self.unicode_digits = "unicode", ""
            elif char in _ESCAPES:
                self._string_add(_ESCAPES[char])
                self.string_state = "normal"
            else:
                self._fail()
        else:
            if char not in _HEX:
                self._fail()
            else:
                self.unicode_digits += char
                if len(self.unicode_digits) == 4:
                    self._string_add(chr(int(self.unicode_digits, 16)))
                    self.string_state = "normal"
        return index + 1

    def _value_start(self, char):
        target = None
        if self.stack:
            frame = self.stack[-1]
            if len(self.stack) == 1 and frame.kind == "object" and frame.key in _TIME_KEYS:
                target = frame.key
            frame.state, frame.key = "comma_or_end", None
        else:
            self.started = True
            self.object_root = char == "{"
        if target is not None and char != '"':
            self._fail("invalid_time_metadata")
            return
        if char in "{[":
            if len(self.stack) >= MAX_JSON_DEPTH:
                self._fail("json_depth_limit")
                return
            self.stack.append(
                _Frame("object", "key_or_end") if char == "{" else _Frame("array", "value_or_end")
            )
        elif char == '"':
            self._string_start(target=target)
        elif char == "-" or "0" <= char <= "9":
            self.mode = "number"
            self.number_state = "minus" if char == "-" else "zero" if char == "0" else "int"
        elif char in "tfn":
            self.mode = "literal"
            self.literal = {"t": "true", "f": "false", "n": "null"}[char]
            self.literal_at = 1
        else:
            self._fail()

    def _number_feed(self, char):
        """Return False to process a completed number's delimiter again."""
        state, digit = self.number_state, "0" <= char <= "9"
        if state == "minus":
            if digit:
                self.number_state = "zero" if char == "0" else "int"
            else:
                self._fail()
        elif state in {"zero", "int", "frac", "exp"}:
            if digit and state != "zero":
                return True
            if char == "." and state in {"zero", "int"}:
                self.number_state = "frac_start"
            elif char in "eE" and state in {"zero", "int", "frac"}:
                self.number_state = "exp_start"
            else:
                self.mode = None
                return False
        elif state == "frac_start":
            if digit:
                self.number_state = "frac"
            else:
                self._fail()
        elif state == "exp_start":
            if char in "+-":
                self.number_state = "exp_sign"
            elif digit:
                self.number_state = "exp"
            else:
                self._fail()
        elif state == "exp_sign":
            if digit:
                self.number_state = "exp"
            else:
                self._fail()
        return True

    def _structural(self, char):
        if char in _WHITESPACE:
            return
        if not self.stack:
            if self.started:
                self._fail()
            else:
                self._value_start(char)
            return
        frame = self.stack[-1]
        if frame.kind == "object":
            if frame.state in {"key_or_end", "key"}:
                if char == "}" and frame.state == "key_or_end":
                    self.stack.pop()
                elif char == '"':
                    self._string_start(key=True)
                else:
                    self._fail()
            elif frame.state == "colon":
                if char == ":":
                    frame.state = "value"
                else:
                    self._fail()
            elif frame.state == "value":
                self._value_start(char)
            elif char == ",":
                frame.state = "key"
            elif char == "}":
                self.stack.pop()
            else:
                self._fail()
        elif frame.state in {"value_or_end", "value"}:
            if char == "]" and frame.state == "value_or_end":
                self.stack.pop()
            else:
                self._value_start(char)
        elif char == ",":
            frame.state = "value"
        elif char == "]":
            self.stack.pop()
        else:
            self._fail()

    def feed(self, text):
        index = 0
        while index < len(text) and self.error is None:
            if self.mode == "string":
                index = self._string_feed(text, index)
                continue
            char = text[index]
            if self.mode == "number":
                if not self._number_feed(char):
                    continue
            elif self.mode == "literal":
                if char != self.literal[self.literal_at]:
                    self._fail()
                else:
                    self.literal_at += 1
                    if self.literal_at == len(self.literal):
                        self.mode = None
            else:
                self._structural(char)
            index += 1

    def finish(self):
        if self.mode == "number" and self.number_state in {"zero", "int", "frac", "exp"}:
            self.mode = None
        if self.mode is not None or self.stack or not self.started:
            self._fail()
        if not self.error and not self.object_root:
            self._fail("json_not_object")
        return self.error


class _RecordScanner:
    def __init__(self, jsonl):
        self.decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self.json = _JsonScanner() if jsonl else None
        self.invalid_utf8 = False

    def feed(self, block, *, final=False):
        if self.invalid_utf8:
            return
        try:
            text = self.decoder.decode(block, final=final)
        except UnicodeDecodeError:
            self.invalid_utf8 = True
            return
        if self.json is not None:
            self.json.feed(text)

    def finish(self, line, start, end):
        self.feed(b"", final=True)
        error = "invalid_utf8" if self.invalid_utf8 else self.json.finish() if self.json else None
        metadata = dict(self.json.metadata) if self.json and not error else {}
        return RecordSpan(line, start, end, metadata, error)


def scan_records(path: str | Path) -> Iterator[RecordSpan]:
    """Yield original byte spans, checking complete JSONL records and short times.

    Markdown is one record numbered 0, even when empty. Other formats use physical
    universal-newline records numbered from 1, matching the existing source ID
    reader; only .jsonl is parsed as JSON. LF, CRLF and bare CR stay byte-exact.
    Invalid records are drained to their line end and carry no trusted
    metadata. An unterminated final line is included. JSON duplicate time keys,
    non-string time values, deep nesting and invalid UTF-8 are explicit errors.
    """
    path = Path(path)
    document = path.suffix.lower() in {".md", ".markdown"}
    jsonl = path.suffix.lower() == ".jsonl"
    scanner = _RecordScanner(jsonl)
    line, start, offset = (0 if document else 1), 0, 0
    pending_cr = False
    with path.open("rb") as stream:
        while block := stream.read(READ_BYTES):
            if document:
                scanner.feed(block)
                offset += len(block)
                continue
            position = 0
            while position < len(block):
                if pending_cr:
                    # Defer a CR until one following byte is available, even
                    # across reads. CRLF belongs to one original record; a
                    # non-LF byte belongs to the next record and is not consumed.
                    if block[position] == 10:
                        scanner.feed(block[position:position + 1])
                        offset += 1
                        position += 1
                    yield scanner.finish(line, start, offset)
                    line, start, scanner = line + 1, offset, _RecordScanner(jsonl)
                    pending_cr = False
                    continue
                newline = _LINE_END.search(block, position)
                stop = len(block) if newline is None else newline.end()
                scanner.feed(block[position:stop])
                offset += stop - position
                position = stop
                if newline is not None:
                    if block[stop - 1] == 13:
                        pending_cr = True
                    else:
                        yield scanner.finish(line, start, offset)
                        line, start, scanner = line + 1, offset, _RecordScanner(jsonl)
        if document or offset > start:
            yield scanner.finish(line, start, offset)


def _utf8_boundary(stream: BinaryIO, start: int, boundary: int, end: int) -> int:
    """Move a proposed cut left only when it bisects a valid UTF-8 sequence."""
    if boundary == end:
        return boundary
    low = max(start, boundary - 3)
    stream.seek(low)
    size = min(end, boundary + 4) - low
    around = stream.read(size)
    if len(around) != size:
        raise ValueError("Source range extends beyond current file length")
    index = boundary - low
    if around[index] & 0xC0 != 0x80:
        return boundary
    lead = index - 1
    while lead >= 0 and around[lead] & 0xC0 == 0x80:
        lead -= 1
    if lead < 0:
        return boundary
    first = around[lead]
    width = 2 if 0xC2 <= first <= 0xDF else 3 if 0xE0 <= first <= 0xEF else 4 if 0xF0 <= first <= 0xF4 else 0
    if not width or lead + width <= index or lead + width > len(around):
        return boundary
    try:
        around[lead : lead + width].decode("utf-8")
    except UnicodeDecodeError:
        return boundary
    cut = low + lead
    return cut if cut > start else boundary


def fragment_ranges(
    path: str | Path, start: int, end: int, max_bytes: int = READ_BYTES
) -> Iterator[FragmentSpan]:
    """Partition a frozen byte range without dropping or repeating a byte.

    Every nonempty fragment is at most max_bytes (minimum 4). Valid UTF-8 stays
    decodable at cuts; invalid bytes remain covered and are explicitly flagged.
    Hashing uses fixed-size reads even when a caller requests huge fragments.
    Empty ranges yield nothing. Source mutation detection belongs to the caller.
    """
    if any(type(value) is not int for value in (start, end, max_bytes)):
        raise ValueError("Byte ranges and fragment limits must be integers")
    if start < 0 or end < start or max_bytes < 4:
        raise ValueError("Invalid byte range or fragment limit below 4")
    with Path(path).open("rb") as stream:
        stream.seek(0, 2)
        if end > stream.tell():
            raise ValueError("Source range extends beyond current file length")
        position = start
        while position < end:
            stop = _utf8_boundary(stream, position, min(position + max_bytes, end), end)
            stream.seek(position)
            digest = hashlib.sha256()
            decoder = codecs.getincrementaldecoder("utf-8")("strict")
            invalid = False
            remaining = stop - position
            while remaining:
                block = stream.read(min(READ_BYTES, remaining))
                if not block:
                    raise ValueError("Source range extends beyond current file length")
                digest.update(block)
                remaining -= len(block)
                if not invalid:
                    try:
                        decoder.decode(block, final=remaining == 0)
                    except UnicodeDecodeError:
                        invalid = True
            yield FragmentSpan(position, stop, digest.hexdigest(), "invalid_utf8" if invalid else None)
            position = stop

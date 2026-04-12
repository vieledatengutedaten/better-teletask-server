"""
Tests for services/translation.py

The VTT parsing/chunking functions are pure string manipulation —
they can be tested directly without mocking.
"""

import pytest
from lib.services.translation import (
    parse_vtt_blocks,
    process_block_timestamps,
    group_blocks_into_chunks,
    TIMESTAMP_LINE_PATTERN,
)


class TestParseVttBlocks:
    def test_separates_header_and_blocks(self):
        raw = (
            "WEBVTT\n\n00:01.000 --> 00:05.000\nHello\n\n00:06.000 --> 00:10.000\nWorld"
        )
        header, blocks = parse_vtt_blocks(raw)
        assert header == "WEBVTT"
        assert len(blocks) == 2
        assert "Hello" in blocks[0]
        assert "World" in blocks[1]

    def test_adds_default_header_if_missing(self):
        raw = "00:01.000 --> 00:05.000\nHello"
        header, blocks = parse_vtt_blocks(raw)
        assert header == "WEBVTT"
        assert len(blocks) == 1

    def test_handles_crlf(self):
        raw = "WEBVTT\r\n\r\n00:01.000 --> 00:05.000\r\nHello"
        header, blocks = parse_vtt_blocks(raw)
        assert header == "WEBVTT"
        assert len(blocks) == 1

    def test_strips_empty_blocks(self):
        raw = "WEBVTT\n\n\n\n00:01.000 --> 00:05.000\nHello\n\n\n\n"
        header, blocks = parse_vtt_blocks(raw)
        assert all(b.strip() for b in blocks)


class TestProcessBlockTimestamps:
    def test_extracts_timestamps_to_placeholders(self):
        blocks = ["00:01.000 --> 00:05.000\nHello world"]
        clean_blocks, ts_map = process_block_timestamps(blocks)

        assert "TS0" in ts_map
        assert "00:01.000" in ts_map["TS0"]
        assert "00:05.000" in ts_map["TS0"]
        # The placeholder should replace the timestamp in the block
        assert "TS0" in clean_blocks[0]
        assert "Hello world" in clean_blocks[0]

    def test_block_without_timestamp_unchanged(self):
        blocks = ["Just some text without timestamps"]
        clean_blocks, ts_map = process_block_timestamps(blocks)
        assert len(ts_map) == 0
        assert clean_blocks[0] == "Just some text without timestamps"

    def test_multiple_blocks(self):
        blocks = [
            "00:01.000 --> 00:05.000\nFirst",
            "00:06.000 --> 00:10.000\nSecond",
            "No timestamp here",
        ]
        clean_blocks, ts_map = process_block_timestamps(blocks)
        assert len(ts_map) == 2
        assert "TS0" in ts_map
        assert "TS1" in ts_map


class TestGroupBlocksIntoChunks:
    def test_single_chunk_if_small(self):
        blocks = ["short text"] * 3
        chunks = group_blocks_into_chunks(blocks, max_chars=1000)
        assert len(chunks) == 1

    def test_splits_when_exceeding_max_chars(self):
        blocks = ["x" * 100] * 10  # 10 blocks, 100 chars each
        chunks = group_blocks_into_chunks(blocks, max_chars=350)
        assert len(chunks) > 1
        # Each chunk should be <= max_chars (approximately)
        for chunk in chunks:
            assert len(chunk) <= 500  # generous margin for separators


class TestTimestampLinePattern:
    def test_matches_standard_format(self):
        line = "00:01.000 --> 00:05.000"
        assert TIMESTAMP_LINE_PATTERN.search(line) is not None

    def test_does_not_match_full_hours_format(self):
        """The current regex only supports MM:SS.mmm, not HH:MM:SS.mmm."""
        line = "01:02:03.456 --> 01:02:10.789"
        # This is expected to NOT match with the current pattern
        assert TIMESTAMP_LINE_PATTERN.search(line) is None

    def test_no_match_on_plain_text(self):
        assert TIMESTAMP_LINE_PATTERN.search("Hello world") is None

"""
Integration tests for services/translation.py

These tests hit a real Ollama instance and are excluded from normal pytest runs.
Run explicitly with:  pytest -m integration
"""

import pytest
from unittest.mock import patch

from lib.services.translation import (
    parse_vtt_blocks,
    process_block_timestamps,
    group_blocks_into_chunks,
    build_translation_prompt,
    query_ollama,
    reinsert_timestamps,
    run_translation_workflow,
)

SAMPLE_VTT = """\
WEBVTT

00:00.000 --> 00:03.500
Willkommen zur Vorlesung.

00:03.500 --> 00:07.000
Heute sprechen wir über Datenbanken.

00:07.000 --> 00:10.000
Bitte öffnen Sie Ihre Unterlagen.
"""


# ── Pure-logic unit tests (no external calls) ──────────────────────


class TestParseVttBlocks:
    def test_header_extraction(self):
        header, blocks = parse_vtt_blocks(SAMPLE_VTT)
        assert header == "WEBVTT"
        assert len(blocks) == 3

    def test_block_content(self):
        _, blocks = parse_vtt_blocks(SAMPLE_VTT)
        assert "Willkommen" in blocks[0]
        assert "Datenbanken" in blocks[1]


class TestProcessBlockTimestamps:
    def test_placeholders_replace_timestamps(self):
        _, blocks = parse_vtt_blocks(SAMPLE_VTT)
        clean_blocks, ts_map = process_block_timestamps(blocks)
        assert "TS0" in clean_blocks[0]
        assert "00:00.000 --> 00:03.500" in ts_map["TS0"]

    def test_all_blocks_get_placeholders(self):
        _, blocks = parse_vtt_blocks(SAMPLE_VTT)
        clean_blocks, ts_map = process_block_timestamps(blocks)
        assert len(ts_map) == 3


class TestGroupBlocksIntoChunks:
    def test_single_chunk_for_small_input(self):
        _, blocks = parse_vtt_blocks(SAMPLE_VTT)
        clean_blocks, _ = process_block_timestamps(blocks)
        chunks = group_blocks_into_chunks(clean_blocks, max_chars=5000)
        assert len(chunks) == 1

    def test_multiple_chunks_with_tiny_limit(self):
        _, blocks = parse_vtt_blocks(SAMPLE_VTT)
        clean_blocks, _ = process_block_timestamps(blocks)
        chunks = group_blocks_into_chunks(clean_blocks, max_chars=30)
        assert len(chunks) >= 2


class TestReinsertTimestamps:
    def test_roundtrip(self):
        _, blocks = parse_vtt_blocks(SAMPLE_VTT)
        clean_blocks, ts_map = process_block_timestamps(blocks)
        restored = reinsert_timestamps(clean_blocks, ts_map)
        assert "00:00.000 --> 00:03.500" in restored[0]
        assert "TS0" not in restored[0]


class TestBuildTranslationPrompt:
    def test_prompt_contains_chunk(self):
        prompt = build_translation_prompt("Hallo Welt", "German", "English")
        assert "Hallo Welt" in prompt
        assert "German" in prompt
        assert "English" in prompt


# ── Integration tests (require running Ollama) ─────────────────────


@pytest.mark.integration
class TestOllamaTranslation:
    """These tests call a real Ollama instance.

    Run with:  pytest -m integration
    Requires:  OLLAMA_URL and OLLAMA_MODEL env vars (or app defaults).
    """

    def test_query_ollama_returns_response(self):
        """Smoke test: check that Ollama responds to a trivial prompt."""
        from lib.core.config import OLLAMA_URL, OLLAMA_MODEL

        result = query_ollama(
            "Translate to English: Hallo Welt", OLLAMA_URL, OLLAMA_MODEL
        )
        assert result is not None
        assert len(result) > 0

    def test_translate_small_vtt_chunk(self):
        """Translate the 3-block sample and verify placeholders survive."""
        from lib.core.config import OLLAMA_URL, OLLAMA_MODEL

        _, blocks = parse_vtt_blocks(SAMPLE_VTT)
        clean_blocks, ts_map = process_block_timestamps(blocks)
        chunk_text = "\n\n".join(clean_blocks)

        prompt = build_translation_prompt(chunk_text, "German", "English")
        translated = query_ollama(prompt, OLLAMA_URL, OLLAMA_MODEL)

        assert translated is not None
        # The placeholders (TS0, TS1, TS2) should survive translation
        for key in ts_map:
            assert key in translated, f"Placeholder {key} was lost during translation"

    def test_full_workflow_with_injected_vtt(self):
        """Run the full translation workflow with an injected VTT string.

        Mocks only file I/O — the real Ollama translation runs.
        Verifies the output VTT has correct structure and timestamps.
        """
        saved_output: dict[str, str] = {}

        def mock_get_original_vtt(config):
            return SAMPLE_VTT

        def mock_save_vtt_file(file_path, header, content_blocks):
            saved_output["file_path"] = file_path
            saved_output["header"] = header
            saved_output["content"] = "\n\n".join(content_blocks)

        with (
            patch(
                "app.services.translation.get_original_vtt",
                side_effect=mock_get_original_vtt,
            ),
            patch(
                "app.services.translation.save_vtt_file",
                side_effect=mock_save_vtt_file,
            ),
        ):
            run_translation_workflow(id=99999, to_langauge="en", from_language="de")

        assert "header" in saved_output, "Workflow did not produce any output"
        assert saved_output["header"] == "WEBVTT"

        output_blocks = saved_output["content"].split("\n\n")

        # Input had 3 subtitle blocks, output should too
        assert (
            len(output_blocks) == 3
        ), f"Expected 3 output blocks, got {len(output_blocks)}"

        # All timestamps must be reinserted (not placeholders)
        for block in output_blocks:
            assert "-->" in block, f"Timestamp missing in block: {block}"
            assert not any(
                f"TS{i}" in block for i in range(10)
            ), f"Placeholder not replaced in block: {block}"

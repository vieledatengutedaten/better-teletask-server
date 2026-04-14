"""Example CLI for transcription job arguments."""

import argparse
from lib.models.jobs.transcription import TranscriptionParams


def run_transcription(
	params: TranscriptionParams,
) -> None:
	_ = params


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Example transcription job CLI")
	_ = parser.add_argument("--teletask-id", type=int, required=True)
	_ = parser.add_argument("--initial-prompt", type=str, default=None)
	_ = parser.add_argument("--asr-model", type=str, default=None)
	_ = parser.add_argument("--compute-type", type=str, default=None)
	return parser


def main() -> None:
	parsed = build_parser().parse_args()
	params = TranscriptionParams.model_validate(vars(parsed))
	run_transcription(params)


if __name__ == "__main__":
	main()



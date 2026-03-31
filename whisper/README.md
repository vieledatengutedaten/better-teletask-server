# Dev Docs

## Package Manager
We use uv. It creates a normal python enviornment in .venv
For every command, use `uv run <command>` orr activate the environment with `source .venv/bin/activate` and then the command

## Formatting
We use Black, either run `uv run black .` or `black .`

## Testing
We use pytest, either run `uv run pytest` or `pytest`

## Running
Run the main server with `uv run uvicorn api.main:app --reload` for hot reloading

## Linter
Based Pyright
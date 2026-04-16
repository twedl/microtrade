# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

This repository is a greenfield Python project. As of this writing it contains only `README.md`, `LICENSE` (MIT), and a Python `.gitignore` — no source code, package layout, dependency manifest, test suite, or tooling config has been committed yet.

The stated purpose (from `README.md`) is "Trade microdata". Any architecture, build commands, or test commands will need to be established as the project is scaffolded; update this file once they exist.

## Conventions

- Language: Python (inferred from `.gitignore`). No `pyproject.toml`, `requirements.txt`, or lockfile has been chosen yet — pick and document one when scaffolding.
- The `.gitignore` already accounts for most common Python toolchains (uv, poetry, pdm, pipenv, pixi, ruff, mypy, pytest), so adopting any of these will not require ignore-file changes.

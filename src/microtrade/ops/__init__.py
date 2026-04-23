"""Operational layer: dirty-check planner, per-file manifests, and the
cron-style runner that drives microtrade's ingest pipeline.

Previously lived in a separate `tp` repo; folded in so the library and
its sole consumer share one version, one CI, one test run.
"""

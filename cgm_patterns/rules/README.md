  usage: run_patterns.py [-h] [--start START] [--end END] [--patterns [PATTERNS ...]]
                       [--output OUTPUT] [--no-progress] [--workers WORKERS]
                       csv_file
                       
  Components

  - Models/Data: PatternInputBundle, PatternContext, PatternDetection, and supporting CGM day/summary dataclasses define the payloads flowing through the system (cgm_patterns/models.py:34,
  cgm_patterns/models.py:122, cgm_patterns/models.py:138). Bundles expose rolling raw/summarized data; context carries global and per-pattern threshold overrides; detections standardize outputs.
  - Rule Base: All detection rules subclass PatternRule, which enforces an id, supplies a reusable descriptor, and exposes helpers like resolved_threshold and ensure_validation_window so individual
  rules stay laser-focused on analytics (cgm_patterns/rule_base.py:16).
  - Registry: RuleRegistry holds instantiated rule objects keyed by id, and the @register_rule decorator wires new subclasses into the registry at definition time while blocking duplicate
  IDs (cgm_patterns/registry.py:12, cgm_patterns/registry.py:60). should_evaluate_rule consults metadata/context to skip rules that aren’t applicable for the current patient (cgm_patterns/
  pattern_metadata.py:608).

  Registration Flow

  - The cgm_patterns.rules package discovers every module under rules/, imports each one, and lets class decorators self-register. Importing the package is all that’s required to populate
  the registry; reload_rules can re-import modules after clearing the registry for hot reloading (cgm_patterns/rules/__init__.py:12, cgm_patterns/rules/__init__.py:30, cgm_patterns/rules/
  __init__.py:37).

  Execution Pipeline

  - SlidingWindowEngine (1) pulls CGM days via a DailyCGMSource, (2) caches/derives daily summaries, (3) builds a PatternInputBundle and PatternContext, then (4) runs registry.detect_all for each
  analysis date, honoring optional rule filters (cgm_patterns/engine.py:34, cgm_patterns/engine.py:64, cgm_patterns/engine.py:125).
  - Batch scripts such as run_patterns.py import the rules package (triggering registration), construct the engine with desired window sizes, stream patients, and capture detections per date
  (cgm_patterns/run_patterns.py:13, cgm_patterns/run_patterns.py:55, cgm_patterns/run_patterns.py:84).

  Rule Implementation Pattern

  - Individual rules (e.g., HighGlycemicVariabilityRule) resolve configurable thresholds from context, inspect the bundle’s prepared CGM data using shared utilities, and return a PatternDetection
  with status, metrics, and human-readable evidence (cgm_patterns/rules/high_glycemic_variability.py:13, cgm_patterns/rules/utils.py:12).
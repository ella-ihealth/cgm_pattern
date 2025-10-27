"""Structured metadata for CGM pattern signatures."""
from __future__ import annotations

import re
from typing import Any, Iterable, Mapping, Sequence, Set

from .models import PatternContext
from .rule_base import PatternRule

PATTERN_METADATA: dict[int, dict[str, object]] = {1: {'pattern_signature_name': 'Predominant Hyperglycemia',
     'signature_type': 'Physiologic',
     'category_tags': ['macro'],
     'cluster_id': 'Macro Control',
     'lay_summary': 'Overall elevated glucose with frequent time-above-range.',
     'clinical_teaching_note': '*Prioritize optimization before fine-tuning.*',
     'usefulness_clinical_significance': 'Correlates with higher A1C and '
                                         'complications; guides therapy '
                                         'escalation.',
     'usefulness_rating': None,
     'requires_context': None,
     'context_types_42_factor_taxonomy': ['Meals', 'Med adherence'],
     'diagnosis_context': 'T2DM/General',
     'interpretive_adjustment_by_dx': 'Lower targets in pregnancy/preDM',
     'rule_definition_1_line': 'TAR >30% on >=40% of days',
     'detection_rule_structured': 'input=CGM; metric=TAR>180; cond=>30%; '
                                  'repeat=GEQ40PCT',
     'analysis_window': '7d',
     'validation_window': '14d',
     'post_event_windows_to_evaluate': 'All day',
     'min_days_of_data_required': '>=7',
     'min_days_meeting_rule_recurrence': '>=40% days',
     'confidence_basis': 'AGP/TIR consensus',
     'signature_confidence': 'High',
     'clinical_action_tier': 'Control',
     'expected_action_tags': ['Med', 'Nutrition'],
     'section': '1-Macro',
     'info_service_command': 'curl --location '
                             "'http://agent-service-svc.ai.svc.cluster.local/draft-chat-response/cgm_info' "
                             "--header 'Content-Type: application/json' --data "
                             "'{\n"
                             '    "user_id": "2",\n'
                             '    "patient_id": "5f494a61019de515e8e2585f"\n'
                             "}'"},
 2: {'pattern_signature_name': 'Predominant Hypoglycemia',
     'signature_type': 'Physiologic',
     'category_tags': ['macro'],
     'cluster_id': 'Macro Safety',
     'lay_summary': 'Frequent/prolonged lows across days.',
     'clinical_teaching_note': '*Fix safety before control.*',
     'usefulness_clinical_significance': 'Indicates overtreatment or timing '
                                         'mismatch; risk of severe events.',
     'usefulness_rating': None,
     'requires_context': None,
     'context_types_42_factor_taxonomy': ['Meds', 'Dosing'],
     'diagnosis_context': 'T2DM/General',
     'interpretive_adjustment_by_dx': 'Stricter in pregnancy',
     'rule_definition_1_line': 'TBR<70 >=4% OR any <54',
     'detection_rule_structured': 'input=CGM; metric=TBR; cond=GEQ4PCT or '
                                  'ANY_<54; repeat=GEQ40PCT',
     'analysis_window': '7d',
     'validation_window': '14d',
     'post_event_windows_to_evaluate': 'All day',
     'min_days_of_data_required': '>=7',
     'min_days_meeting_rule_recurrence': '>=40% days',
     'confidence_basis': 'ADA time-in-range targets',
     'signature_confidence': 'High',
     'clinical_action_tier': 'Safety',
     'expected_action_tags': ['Med', 'Education'],
     'section': '1-Macro'},
 3: {'pattern_signature_name': 'High Glycemic Variability',
     'signature_type': 'Physiologic',
     'category_tags': ['macro', 'variability'],
     'cluster_id': 'Variability',
     'lay_summary': 'Large day-to-day swings/high IQR bands.',
     'clinical_teaching_note': '*Stabilize patterns before intensifying meds.*',
     'usefulness_clinical_significance': 'Linked to symptoms and '
                                         'complications; aim to reduce '
                                         'variability.',
     'usefulness_rating': None,
     'requires_context': None,
     'context_types_42_factor_taxonomy': [],
     'diagnosis_context': 'T2DM/General',
     'interpretive_adjustment_by_dx': None,
     'rule_definition_1_line': 'CV >=36% repeatedly',
     'detection_rule_structured': 'input=CGM; metric=CV; cond=>=36%; '
                                  'repeat=GEQ40PCT',
     'analysis_window': '7d',
     'validation_window': '14d',
     'post_event_windows_to_evaluate': 'All day',
     'min_days_of_data_required': '>=7',
     'min_days_meeting_rule_recurrence': '>=40% days',
     'confidence_basis': 'AGP CV heuristic',
     'signature_confidence': 'High',
     'clinical_action_tier': 'Control',
     'expected_action_tags': ['Education', 'Nutrition', 'Med'],
     'section': '1-Macro'},
 4: {'pattern_signature_name': 'Stable / Near-Target Control',
     'signature_type': 'Physiologic',
     'category_tags': ['macro', 'stability'],
     'cluster_id': 'Macro Stability',
     'lay_summary': 'Good overall control with narrow AGP bands.',
     'clinical_teaching_note': '*Reinforce effective routines.*',
     'usefulness_clinical_significance': 'Supports maintenance, consider '
                                         'de-intensification if appropriate.',
     'usefulness_rating': None,
     'requires_context': None,
     'context_types_42_factor_taxonomy': [],
     'diagnosis_context': 'T2DM/General',
     'interpretive_adjustment_by_dx': None,
     'rule_definition_1_line': 'TIR >=70% and CV <36%',
     'detection_rule_structured': 'input=CGM; metric=COMBO(TIR,CV); '
                                  'cond=(TIR>=70 & CV<36); repeat=GEQ40PCT',
     'analysis_window': '7d',
     'validation_window': '14d',
     'post_event_windows_to_evaluate': 'All day',
     'min_days_of_data_required': '>=7',
     'min_days_meeting_rule_recurrence': '>=40% days',
     'confidence_basis': 'AGP norms',
     'signature_confidence': 'High',
     'clinical_action_tier': 'Lifestyle',
     'expected_action_tags': ['Education'],
     'section': '1-Macro'},
5: {'pattern_signature_name': 'Nocturnal Hypoglycemia (00:00-06:00)',
     'signature_type': 'Physiologic',
     'category_tags': ['micro', 'safety'],
     'cluster_id': 'Time-of-Day Hypoglycemia',
     'lay_summary': 'Lows during sleep period.',
     'clinical_teaching_note': '*Adjust basal/corrections; consider HS snack '
                               'policy.*',
     'usefulness_clinical_significance': 'Prevent nocturnal events.',
     'usefulness_rating': None,
     'requires_context': None,
     'context_types_42_factor_taxonomy': ['Meds', 'Meals'],
     'diagnosis_context': 'T2DM/General',
     'interpretive_adjustment_by_dx': None,
     'rule_definition_1_line': '≥15 minutes <70 mg/dL (or any <54) on ≥2 nights '
                               'within last 14 days',
     'detection_rule_structured': 'input=CGM; window=clock[00:00-06:00]; '
                                  'metric=TIME_<70 & MIN_<54; '
                                  'cond=(TIME_<70>=15min OR MIN_<54); '
                                  'repeat=GEQ2 nights/14d',
     'analysis_window': '14d',
     'validation_window': '14d',
     'post_event_windows_to_evaluate': '[00:00-06:00]',
     'min_days_of_data_required': '>=7',
     'min_days_meeting_rule_recurrence': '>=2 nights',
     'confidence_basis': 'TBR-based heuristic',
     'signature_confidence': 'High',
     'clinical_action_tier': 'Safety',
     'expected_action_tags': ['Med', 'Education'],
     'section': '2-Micro Hypoglycemia'},
 12: {'pattern_signature_name': 'Somogyi Effect (Rebound Morning High)',
      'signature_type': 'Pharmacologic',
      'category_tags': ['micro', 'circadian', 'safety'],
      'cluster_id': 'Overnight Regulation',
      'lay_summary': 'Overnight low triggers rebound morning high.',
      'clinical_teaching_note': '*Differentiate from dawn before changing '
                                'basal.*',
      'usefulness_clinical_significance': 'Prevents wrong insulin adjustments.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Medication timing'],
      'diagnosis_context': 'T2DM (insulin users)',
      'interpretive_adjustment_by_dx': None,
      'rule_definition_1_line': 'Low <70 >=15min then rise >=100 before '
                                'breakfast',
      'detection_rule_structured': 'input=CGM; window=clock[00:00-08:00]; '
                                   'metric=SEQUENCE_LOW_THEN_HIGH; '
                                   'cond=(LOW<70>=15min THEN rise>=100 within '
                                   '2-4h); repeat=GEQ2D',
      'analysis_window': '7d',
      'validation_window': '14d',
     'post_event_windows_to_evaluate': 'Overnight->Morning',
      'min_days_of_data_required': '>=7',
      'min_days_meeting_rule_recurrence': '>=2',
      'confidence_basis': 'Sequence heuristic + clinical literature',
      'signature_confidence': 'Medium',
      'clinical_action_tier': 'Safety',
      'expected_action_tags': ['Med', 'Timing', 'Education'],
      'section': '2-Micro Hypoglycemia'},
14: {'pattern_signature_name': 'Dawn Phenomenon',
     'signature_type': 'Physiologic',
     'category_tags': ['micro', 'time-of-day'],
     'cluster_id': 'Overnight Regulation',
     'lay_summary': 'Early-morning rise from nocturnal nadir without overnight lows.',
     'clinical_teaching_note': '*Consider basal timing; review bedtime snacks.*',
     'usefulness_clinical_significance': 'Explains fasting highs driven by counter-regulation.',
     'usefulness_rating': None,
     'requires_context': None,
     'context_types_42_factor_taxonomy': ['Meals', 'Circadian'],
     'diagnosis_context': 'T2DM/General',
     'interpretive_adjustment_by_dx': None,
     'rule_definition_1_line': 'Rise >20 mg/dL from nocturnal nadir to pre-breakfast with no overnight lows.',
     'detection_rule_structured': 'input=CGM; window=clock[00:00-08:00]; '
                                  'metric=RISE(nadir->prebreakfast); '
                                  'cond=rise>20 & overnight>=70; '
                                  'repeat=GEQ3 mornings/5d',
     'analysis_window': '5d',
     'validation_window': '14d',
     'post_event_windows_to_evaluate': '[03:00-08:00]',
     'min_days_of_data_required': '>=7',
     'min_days_meeting_rule_recurrence': '>=3 mornings',
     'confidence_basis': 'CGM nadir-to-morning rise',
     'signature_confidence': 'High',
     'clinical_action_tier': 'Control',
     'expected_action_tags': ['Nutrition', 'Timing', 'Med'],
     'section': '3-Micro Hyperglycemia'},
 24: {'pattern_signature_name': 'Weekday vs Weekend Instability',
      'signature_type': 'Behavioral',
      'category_tags': ['variability', 'lifestyle'],
      'cluster_id': 'Circadian/Behavioral',
      'lay_summary': 'Stable weekdays but unstable weekends (or vice versa).',
      'clinical_teaching_note': '*Personalize weekend strategies.*',
      'usefulness_clinical_significance': 'Targets specific days for '
                                          'intervention.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Schedule', 'Behavior'],
      'diagnosis_context': 'T2DM/General',
      'interpretive_adjustment_by_dx': None,
      'rule_definition_1_line': 'Weekend CV or TAR > weekday baseline + '
                                'threshold',
      'detection_rule_structured': 'input=CGM; metric=CV/TAR; cond=weekend >= '
                                   'weekday + delta; repeat=GEQ2 weekends',
      'analysis_window': '14d',
      'validation_window': '30d',
      'post_event_windows_to_evaluate': 'Weekends',
      'min_days_of_data_required': '>=14',
      'min_days_meeting_rule_recurrence': '>=2 weekends',
      'confidence_basis': 'Comparative AGP',
      'signature_confidence': 'Medium',
      'clinical_action_tier': 'Lifestyle',
      'expected_action_tags': ['Education', 'Behavior'],
      'section': '4-Event-Based Variability & Outliers'},
 25: {'pattern_signature_name': 'Evening Variability Spike (18:00-22:00)',
      'signature_type': 'Physiologic',
      'category_tags': ['variability', 'time-of-day'],
      'cluster_id': 'Variability',
      'lay_summary': 'Higher variance and swings in evening block.',
      'clinical_teaching_note': '*Often linked to large dinners or stress.*',
      'usefulness_clinical_significance': 'Focus stabilization where '
                                          'volatility is highest.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': [],
      'diagnosis_context': 'T2DM/General',
      'interpretive_adjustment_by_dx': None,
      'rule_definition_1_line': 'IQR>40 mg/dL OR CV>36% repeatedly',
      'detection_rule_structured': 'input=CGM; window=clock[18:00-22:00]; '
                                   'metric=IQR & CV; cond=IQR>40 or CV>36; '
                                   'repeat=GEQ40PCT',
      'analysis_window': '7d',
      'validation_window': '14d',
      'post_event_windows_to_evaluate': 'Evening',
      'min_days_of_data_required': '>=7',
      'min_days_meeting_rule_recurrence': '>=40% days',
      'confidence_basis': 'AGP percentile bands',
      'signature_confidence': 'Medium',
      'clinical_action_tier': 'Optimization',
      'expected_action_tags': ['Nutrition', 'Education'],
      'section': '4-Event-Based Variability & Outliers'},
 26: {'pattern_signature_name': 'Single-Day High Spike (>300 mg/dL)',
      'signature_type': 'Physiologic',
      'category_tags': ['micro', 'variability', 'outlier'],
      'cluster_id': 'Event-Based Variability & Outliers',
      'lay_summary': 'One-time extreme high glucose event (>300 mg/dL) not '
                     'sustained over following days.',
      'clinical_teaching_note': 'Confirm if recurrent; may reflect missed dose '
                                'or heavy meal.',
      'usefulness_clinical_significance': 'Detects acute, clinically relevant '
                                          'spikes that may not affect averages '
                                          'but warrant review.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Meal timing (optional)'],
      'diagnosis_context': 'T2DM',
      'interpretive_adjustment_by_dx': 'Similar threshold for preDM/T2DM.',
      'rule_definition_1_line': 'Isolated spike >300 lasting <2h.',
      'detection_rule_structured': 'max_glucose_day >300 AND '
                                   'duration_above_250 <120min',
      'analysis_window': '7d',
      'validation_window': '14d',
      'post_event_windows_to_evaluate': 'All-day',
      'min_days_of_data_required': '>=7',
      'min_days_meeting_rule_recurrence': '>=1',
      'confidence_basis': 'Threshold-based',
      'signature_confidence': 'Medium',
      'clinical_action_tier': 'Safety',
      'expected_action_tags': ['Medication / Nutrition'],
      'section': '4-Event-Based Variability & Outliers'},
 27: {'pattern_signature_name': 'Single-Day Low (<54 mg/dL)',
      'signature_type': 'Physiologic',
      'category_tags': ['micro', 'variability', 'outlier'],
      'cluster_id': 'Event-Based Variability & Outliers',
      'lay_summary': 'One-time episode of severe hypoglycemia without '
                     'repetition.',
      'clinical_teaching_note': 'Review for sulfonylurea or insulin '
                                'overcorrection.',
      'usefulness_clinical_significance': 'Important for identifying isolated '
                                          'dangerous lows even when TBR <1%.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Medication use (optional)'],
      'diagnosis_context': 'T2DM',
      'interpretive_adjustment_by_dx': 'Less likely in non-insulin users.',
      'rule_definition_1_line': 'Any day with time_below_54 >15min.',
      'detection_rule_structured': 'time_below_54 >15min on 1 day in 14',
      'analysis_window': '14d',
      'validation_window': '30d',
      'post_event_windows_to_evaluate': 'All-day',
      'min_days_of_data_required': '>=14',
      'min_days_meeting_rule_recurrence': '>=1',
      'confidence_basis': 'ADA <54 threshold',
      'signature_confidence': 'High',
      'clinical_action_tier': 'Safety',
      'expected_action_tags': ['Medication / Education'],
      'section': '4-Event-Based Variability & Outliers'},
 29: {'pattern_signature_name': 'Rapid Rise (>80 mg/dL / 15 min)',
      'signature_type': 'Physiologic',
      'category_tags': ['micro', 'variability', 'outlier'],
      'cluster_id': 'Event-Based Variability & Outliers',
      'lay_summary': 'Rapid glucose increase suggesting high GI food or stress '
                     'surge.',
      'clinical_teaching_note': 'Consider meal composition or emotional stress '
                                'triggers.',
      'usefulness_clinical_significance': 'Early identifier of reactive '
                                          'hyperglycemia.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Optional: meal timing'],
      'diagnosis_context': 'T2DM',
      'interpretive_adjustment_by_dx': 'Same across Dx.',
      'rule_definition_1_line': 'Rise >80 mg/dL in 15 min.',
      'detection_rule_structured': 'delta_glucose_15min >80',
      'analysis_window': '7d',
      'validation_window': '14d',
      'post_event_windows_to_evaluate': '0-3h post-meal',
      'min_days_of_data_required': '>=7',
      'min_days_meeting_rule_recurrence': '>=3',
      'confidence_basis': 'AGP variability bands',
      'signature_confidence': 'Medium',
      'clinical_action_tier': 'Optimization',
      'expected_action_tags': ['Nutrition / Behavior'],
      'section': '4-Event-Based Variability & Outliers'},
 30: {'pattern_signature_name': 'Rapid Fall (>60 mg/dL / 15 min)',
      'signature_type': 'Physiologic',
      'category_tags': ['micro', 'variability', 'outlier'],
      'cluster_id': 'Event-Based Variability & Outliers',
      'lay_summary': 'Sharp glucose drop likely linked to insulin timing or '
                     'activity.',
      'clinical_teaching_note': 'Warns of hypoglycemia risk post insulin or '
                                'exercise.',
      'usefulness_clinical_significance': 'Highlights overcorrection or '
                                          'sensitivity patterns.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Exercise timing'],
      'diagnosis_context': 'T2DM',
      'interpretive_adjustment_by_dx': 'More frequent in insulin users.',
      'rule_definition_1_line': 'Fall >60 mg/dL in 15 min.',
      'detection_rule_structured': 'delta_glucose_15min < -60',
      'analysis_window': '7d',
      'validation_window': '14d',
      'post_event_windows_to_evaluate': '0-3h post-meal or post-exercise',
      'min_days_of_data_required': '>=7',
      'min_days_meeting_rule_recurrence': '>=3',
      'confidence_basis': 'AGP variability bands',
      'signature_confidence': 'Medium',
      'clinical_action_tier': 'Safety',
      'expected_action_tags': ['Medication / Exercise'],
      'section': '4-Event-Based Variability & Outliers'},
 31: {'pattern_signature_name': 'Single Long High (>250 mg/dL >4h once)',
      'signature_type': 'Physiologic',
      'category_tags': ['micro', 'variability', 'outlier'],
      'cluster_id': 'Event-Based Variability & Outliers',
      'lay_summary': 'One-off prolonged hyperglycemia lasting >4h without '
                     'repeat.',
      'clinical_teaching_note': 'Investigate skipped meds, illness, or stress '
                                'day.',
      'usefulness_clinical_significance': 'Detects rare sustained highs that '
                                          'may distort averages.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Optional: illness, med timing'],
      'diagnosis_context': 'T2DM',
      'interpretive_adjustment_by_dx': 'Same across Dx.',
      'rule_definition_1_line': 'Any day with >4h above 250.',
      'detection_rule_structured': 'time_above_250 >=240min on 1 day',
      'analysis_window': '14d',
      'validation_window': '30d',
      'post_event_windows_to_evaluate': 'All-day',
      'min_days_of_data_required': '>=14',
      'min_days_meeting_rule_recurrence': '>=1',
      'confidence_basis': 'Single-day threshold',
      'signature_confidence': 'Medium',
      'clinical_action_tier': 'Control',
      'expected_action_tags': ['Medication / Education'],
      'section': '4-Event-Based Variability & Outliers'},
 32: {'pattern_signature_name': 'Day-to-Day Instability (Single-day CV >36%)',
      'signature_type': 'Statistical',
      'category_tags': ['micro', 'variability', 'outlier'],
      'cluster_id': 'Event-Based Variability & Outliers',
      'lay_summary': 'Glucose highly unstable within one day but averages '
                     'normalize over week.',
      'clinical_teaching_note': 'Look for lifestyle or stress-related drivers.',
      'usefulness_clinical_significance': 'Identifies erratic days that '
                                          'increase overall glycemic risk.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Stress, meal irregularity'],
      'diagnosis_context': 'T2DM',
      'interpretive_adjustment_by_dx': 'Similar across Dx.',
      'rule_definition_1_line': 'Any day with CV >36% but 7d mean CV <36%.',
      'detection_rule_structured': 'daily_cv >36 AND mean_7d_cv <36',
      'analysis_window': '7d',
      'validation_window': '14d',
      'post_event_windows_to_evaluate': 'All-day',
      'min_days_of_data_required': '>=7',
      'min_days_meeting_rule_recurrence': '>=2',
      'confidence_basis': 'CV-based',
      'signature_confidence': 'Medium',
      'clinical_action_tier': 'Optimization',
      'expected_action_tags': ['Education / Behavior'],
      'section': '4-Event-Based Variability & Outliers'},
 33: {'pattern_signature_name': 'Implausible Rate-of-Change',
      'signature_type': 'Data Integrity',
      'category_tags': ['data-quality'],
      'cluster_id': 'Sensor/Device Data Quality',
      'lay_summary': 'Physiology-violating ROC.',
      'clinical_teaching_note': '*Run data-quality checks before clinical '
                                'rules.*',
      'usefulness_clinical_significance': 'Removes artifacts to prevent false '
                                          'inferences.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Device/Sensor'],
      'diagnosis_context': 'General',
      'interpretive_adjustment_by_dx': None,
     'rule_definition_1_line': '|delta| >5 mg/dL/min for >=10min.',
     'detection_rule_structured': 'input=CGM; metric=ROC; cond=|delta|>5 for '
                                   '>=10; repeat=GEQ1D',
      'analysis_window': '7d',
      'validation_window': '14d',
      'post_event_windows_to_evaluate': 'Varies',
      'min_days_of_data_required': '>=1',
      'min_days_meeting_rule_recurrence': 'Per rule',
      'confidence_basis': 'Empirical device behavior',
      'signature_confidence': 'High',
      'clinical_action_tier': 'Data / Quality',
      'expected_action_tags': ['Data', 'Device', 'Education'],
      'section': '4-Event-Based Variability & Outliers'},
 34: {'pattern_signature_name': 'Sensor Swap Step-Change',
      'signature_type': 'Data Integrity',
      'category_tags': ['data-quality'],
      'cluster_id': 'Sensor/Device Data Quality',
      'lay_summary': 'Abrupt shift at new sensor start.',
      'clinical_teaching_note': '*Run data-quality checks before clinical '
                                'rules.*',
      'usefulness_clinical_significance': 'Removes artifacts to prevent false '
                                          'inferences.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Device/Sensor'],
      'diagnosis_context': 'General',
      'interpretive_adjustment_by_dx': None,
      'rule_definition_1_line': 'Baseline shift >=25 mg/dL at swap.',
      'detection_rule_structured': 'input=CGM; window=+/-2h swap; '
                                   'metric=LEVEL_STEP; cond=>=25; repeat=GEQ1 '
                                   'EVENT',
      'analysis_window': '7d',
      'validation_window': '14d',
      'post_event_windows_to_evaluate': 'Varies',
      'min_days_of_data_required': '>=1',
      'min_days_meeting_rule_recurrence': 'Per rule',
      'confidence_basis': 'Empirical device behavior',
      'signature_confidence': 'High',
      'clinical_action_tier': 'Data / Quality',
      'expected_action_tags': ['Data', 'Device', 'Education'],
      'section': '4-Event-Based Variability & Outliers'},
35: {'pattern_signature_name': 'Noisy Sensor Day (Exclude)',
      'signature_type': 'Data Integrity',
      'category_tags': ['data-quality'],
      'cluster_id': 'Sensor/Device Data Quality',
      'lay_summary': 'Whole day too noisy to trust.',
      'clinical_teaching_note': '*Run data-quality checks before clinical '
                                'rules.*',
      'usefulness_clinical_significance': 'Removes artifacts to prevent false '
                                          'inferences.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Device/Sensor'],
      'diagnosis_context': 'General',
      'interpretive_adjustment_by_dx': None,
      'rule_definition_1_line': 'Noise index >95th percentile.',
      'detection_rule_structured': 'input=CGM; metric=NOISE_IDX; cond=>95th '
                                   'pct; repeat=GEQ1D',
      'analysis_window': '7d',
      'validation_window': '14d',
      'post_event_windows_to_evaluate': 'Varies',
      'min_days_of_data_required': '>=1',
      'min_days_meeting_rule_recurrence': 'Per rule',
      'confidence_basis': 'Empirical device behavior',
      'signature_confidence': 'High',
      'clinical_action_tier': 'Data / Quality',
      'expected_action_tags': ['Data', 'Device', 'Education'],
      'section': '4-Event-Based Variability & Outliers'},
36: {'pattern_signature_name': 'Frequent Hypoglycemia',
      'signature_type': 'Physiologic',
      'category_tags': ['micro', 'safety'],
      'cluster_id': 'Macro Safety',
      'lay_summary': 'Recurrent lows despite overall control.',
      'clinical_teaching_note': '*Review basal dosing, snacks, and activity.*',
      'usefulness_clinical_significance': 'Frequent lows increase acute risk '
                                          'for severe hypoglycemia.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Meds', 'Meals', 'Activity'],
      'diagnosis_context': 'T2DM/General',
      'interpretive_adjustment_by_dx': 'More stringent targets for pregnancy '
                                       'and older adults.',
      'rule_definition_1_line': 'TBR >4% or ≥4 hypos/week; ≥2 level-2 hypos '
                                 'over 14 days.',
      'detection_rule_structured': 'input=CGM; window=clock[00:00-24:00]; '
                                   'metric=TBR & hypo_events; '
                                   'cond=(TBR>4 OR hypos>=4/week OR '
                                   'level2>=2/14d); repeat=GEQweekly',
      'analysis_window': '7d',
      'validation_window': '14d',
      'post_event_windows_to_evaluate': 'All day',
      'min_days_of_data_required': '>=7',
      'min_days_meeting_rule_recurrence': '>=2 events',
      'confidence_basis': 'TBR thresholds and level-2 episodes',
      'signature_confidence': 'Medium',
      'clinical_action_tier': 'Safety',
      'expected_action_tags': ['Medication', 'Education'],
      'section': '2-Micro Hypoglycemia'},
37: {'pattern_signature_name': 'Morning Hypoglycemia (05:00-09:00)',
      'short_definition': 'CGM <70 between 09:00-11:30 on ≥2 days/14d.',
      'long_definition': 'Recurrent glucose <70 mg/dL during the morning period 05:00-09:00, occurring before or shortly after waking. This pattern reflects residual overnight insulin effect, delayed or missed breakfast, or early-morning activity without sufficient carbohydrate intake. It is considered clinically significant when glucose <70 mg/dL persists for ≥15 minutes on ≥2 separate mornings within a 14-day period. Any occurrence of glucose <54 mg/dL during this window is considered clinically significant and high-risk, regardless of duration, but should be verified to exclude sensor or compression error.',
      'signature_type': 'Physiologic',
      'category_tags': ['micro', 'safety'],
      'cluster_id': 'Time-of-Day Hypoglycemia',
      'lay_summary': 'Recurrent morning lows before or soon after waking.',
      'clinical_teaching_note': '*Review basal insulin timing, breakfast habits, and early activity.*',
      'usefulness_clinical_significance': 'Highlights early-morning lows that increase symptomatic risk and disrupt morning routines.',
      'usefulness_rating': None,
      'requires_context': None,
      'context_types_42_factor_taxonomy': ['Meds', 'Meals', 'Activity'],
      'diagnosis_context': 'T2DM/General',
      'interpretive_adjustment_by_dx': 'Consider tighter thresholds in pregnancy or other high-risk scenarios.',
      'rule_definition_1_line': 'CGM <70 mg/dL ≥15 min (or any <54) during 05:00-09:00 on ≥2 mornings within 14 days.',
      'detection_rule_structured': 'input=CGM; window=clock[05:00-09:00]; metric=TIME_<70 & MIN_<54; cond=(TIME_<70>=15min OR MIN_<54); repeat=GEQ2 mornings/14d',
      'analysis_window': '14d',
      'validation_window': '14d',
      'post_event_windows_to_evaluate': '[05:00-09:00]',
      'min_days_of_data_required': '>=7',
      'min_days_meeting_rule_recurrence': '>=2 mornings',
      'confidence_basis': 'Time-in-range consensus and hypoglycemia safety guidance.',
      'signature_confidence': 'Medium',
      'clinical_action_tier': 'Safety',
      'expected_action_tags': ['Medication', 'Nutrition', 'Education'],
      'section': '2-Micro Hypoglycemia'}}


_TOKEN_SPLIT_PATTERN = re.compile(r"[\\s,;/]+")


def _flatten_values(value: Any) -> Iterable[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        flattened: list[str] = []
        for item in value.values():
            flattened.extend(_flatten_values(item))
        return flattened
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        flattened: list[str] = []
        for item in value:
            flattened.extend(_flatten_values(item))
        return flattened
    return [str(value)]


def _tokenize(value: Any) -> Set[str]:
    tokens: set[str] = set()
    for entry in _flatten_values(value):
        for token in _TOKEN_SPLIT_PATTERN.split(entry):
            cleaned = token.strip().lower()
            if cleaned:
                tokens.add(cleaned)
    return tokens


def resolve_rule_metadata(rule: PatternRule) -> Mapping[str, Any] | None:
    """Return metadata associated with a rule, if any."""

    metadata = getattr(rule, "metadata", None)
    if metadata:
        return metadata
    pattern_id = getattr(rule, "pattern_id", None)
    if pattern_id is not None:
        return PATTERN_METADATA.get(pattern_id)
    return None


def should_evaluate_rule(rule: PatternRule, context: PatternContext) -> bool:
    """Determine whether a rule is applicable given the provided context."""

    metadata = resolve_rule_metadata(rule)
    if not metadata:
        return True

    diag_requirement = metadata.get("diagnosis_context")
    if diag_requirement:
        required_tokens = _tokenize(diag_requirement)
        if required_tokens:
            extras = context.extras or {}
            available_tokens = _tokenize(
                extras.get("diagnosis_context")
                or extras.get("diagnosis_contexts")
                or extras.get("diagnoses")
            )
            if available_tokens and required_tokens.isdisjoint(available_tokens) and "general" not in required_tokens:
                return False

    return True


__all__ = [
    "PATTERN_METADATA",
    "resolve_rule_metadata",
    "should_evaluate_rule",
]

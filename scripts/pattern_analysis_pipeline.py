"""General pipeline for visualizing CGM pattern detections.

This script provides reusable helpers for loading detection exports, slicing CGM
data, aligning windows, and clustering traces. Patterns can register custom
adapters that describe how to extract the relevant window and alignment mode.

Usage example:

```
python scripts/pattern_analysis_pipeline.py \
    afternoon_hypoglycemia \
    "/Users/.../detection_v1/**/*.json" \
    /tmp/afternoon_plots
```

The script writes Plotly HTML files and a CSV summary into the output
directory.
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Sequence

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sklearn.cluster import KMeans

repo_root = Path(__file__).resolve().parents[1]
if repo_root.as_posix() not in sys.path:
    sys.path.insert(0, repo_root.as_posix())

from cgm_patterns.CGM_fetcher import iter_cgm_days  # type: ignore
from cgm_patterns.models import CGMDay


@dataclass
class PatternAdapter:
    """Holds functions/config for slicing and aligning pattern examples."""

    slice_window: Callable[[pd.DataFrame, dict, dict], pd.DataFrame]
    alignment_modes: Sequence[str]
    window_start_key: str | None = None
    window_end_key: str | None = None
    minutes_threshold: float | None = None


def load_pattern_examples(
    pattern_id: str,
    detections_glob: str,
    *,
    patient_filter: str | None = None,
) -> list[dict]:
    """Return every evidence example for the requested pattern."""

    examples: list[dict] = []
    for path in glob.glob(detections_glob, recursive=True):
        payload = json.loads(Path(path).read_text())
        for patient_id, patient_payload in payload.items():
            if patient_filter and patient_id != patient_filter:
                continue
            for detections in patient_payload.get("detections", {}).values():
                for detection in detections:
                    if detection.get("pattern_id") != pattern_id:
                        continue
                    for example in detection.get("evidence", {}).get("examples", []):
                        examples.append(
                            {
                                "patient_id": patient_id,
                                "example": example,
                                "metrics": detection.get("metrics", {}),
                                "source": path,
                            }
                        )
    return examples


def fetch_day(patient_id: str, service_date: datetime) -> CGMDay | None:
    start = service_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    for day in iter_cgm_days(patient_id, start=start, end=end):
        if day.service_date == service_date.date():
            return day
    return None


def prepare_day_frame(day: CGMDay) -> pd.DataFrame:
    frame = day.readings.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    if day.local_timezone:
        try:
            frame["local_time"] = frame["timestamp"].dt.tz_convert(day.local_timezone)
        except Exception:
            frame["local_time"] = frame["timestamp"]
    else:
        frame["local_time"] = frame["timestamp"]
    frame = frame.sort_values("local_time").reset_index(drop=True)

    diffs = frame["local_time"].diff().dt.total_seconds().div(60.0)
    fallback = diffs.median(skipna=True)
    if math.isnan(fallback) or fallback <= 0:
        fallback = 5.0
    frame["minutes"] = diffs.fillna(fallback)
    return frame


def slice_by_hours(frame: pd.DataFrame, start_hour: float, end_hour: float, *, padding_minutes: float = 0.0) -> pd.DataFrame:
    if frame.empty:
        return frame
    hours = frame["local_time"].dt.hour + frame["local_time"].dt.minute / 60.0
    padding_hours = padding_minutes / 60.0
    if end_hour >= start_hour:
        mask = (hours >= start_hour - padding_hours) & (hours <= end_hour + padding_hours)
    else:  # wrap around midnight
        mask = (hours >= start_hour - padding_hours) | (hours <= end_hour + padding_hours)
    return frame.loc[mask].copy()


def add_relative_minutes(window: pd.DataFrame, start_hour: float) -> pd.DataFrame:
    if window.empty:
        return window
    base = window["local_time"].iloc[0].replace(hour=0, minute=0, second=0, microsecond=0)
    window["relative_minutes"] = (window["local_time"] - (base + timedelta(hours=start_hour))).dt.total_seconds() / 60.0
    return window


def align_on_nadir(frames: list[pd.DataFrame], freq_minutes: int) -> tuple[np.ndarray | None, np.ndarray | None]:
    shifted: list[tuple[np.ndarray, np.ndarray]] = []
    min_shift, max_shift = float("inf"), float("-inf")
    for frame in frames:
        rel = frame["relative_minutes"].to_numpy()
        glucose = frame["glucose_mg_dL"].to_numpy()
        if len(rel) < 3:
            continue
        idx = np.argmin(glucose)
        shift_rel = rel - rel[idx]
        order = np.argsort(shift_rel)
        shift_rel, glucose = shift_rel[order], glucose[order]
        min_shift = min(min_shift, shift_rel[0])
        max_shift = max(max_shift, shift_rel[-1])
        shifted.append((shift_rel, glucose))
    if not shifted:
        return None, None
    grid = np.arange(np.floor(min_shift), np.ceil(max_shift) + freq_minutes, freq_minutes)
    aligned = [np.interp(grid, sr, sg, left=np.nan, right=np.nan) for sr, sg in shifted]
    return grid, np.vstack(aligned)


def align_on_window_start(frames: list[pd.DataFrame], duration_hours: float, freq_minutes: int) -> tuple[np.ndarray | None, np.ndarray | None]:
    duration_minutes = int(duration_hours * 60)
    grid = np.arange(0, duration_minutes + freq_minutes, freq_minutes)
    aligned = []
    for frame in frames:
        rel = frame["relative_minutes"].to_numpy()
        glucose = frame["glucose_mg_dL"].to_numpy()
        if len(rel) < 2:
            continue
        order = np.argsort(rel)
        aligned.append(np.interp(grid, rel[order], glucose[order], left=np.nan, right=np.nan))
    if not aligned:
        return None, None
    return grid, np.vstack(aligned)


def summarize_profile(stacked: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    median = np.nanmedian(stacked, axis=0)
    p10 = np.nanpercentile(stacked, 10, axis=0)
    p90 = np.nanpercentile(stacked, 90, axis=0)
    count = np.sum(~np.isnan(stacked), axis=0)
    return median, p10, p90, count


def afternoon_slice(frame: pd.DataFrame, example: dict, metrics: dict) -> pd.DataFrame:
    start_hour = float(metrics.get("window_start_hour", 12.0))
    end_hour = float(metrics.get("window_end_hour", 17.0))
    window = slice_by_hours(frame, start_hour, end_hour)
    if window.empty:
        return window
    low_mask = window["glucose_mg_dL"] < 70.0
    minutes_low = window.loc[low_mask, "minutes"].sum()
    if minutes_low < 15.0:
        return pd.DataFrame()
    return window


def nocturnal_slice(frame: pd.DataFrame, example: dict, metrics: dict) -> pd.DataFrame:
    start_hour = float(metrics.get("sleep_window_start", 0.0))
    end_hour = float(metrics.get("sleep_window_end", 6.0))
    return slice_by_hours(frame, start_hour, end_hour)


PATTERN_ADAPTERS: dict[str, PatternAdapter] = {
    "afternoon_hypoglycemia": PatternAdapter(
        slice_window=afternoon_slice,
        alignment_modes=("window_start",),
        window_start_key="window_start_hour",
        window_end_key="window_end_hour",
        minutes_threshold=15.0,
    ),
    "nocturnal_hypoglycemia_moderate": PatternAdapter(
        slice_window=nocturnal_slice,
        alignment_modes=("nadir", "window_start"),
        window_start_key="sleep_window_start",
        window_end_key="sleep_window_end",
        minutes_threshold=15.0,
    ),
}


def plot_overlay(frames: list[pd.DataFrame], pattern_id: str, output_dir: Path) -> None:
    combined = pd.concat(frames, ignore_index=True)

    overlay = go.Figure()
    for frame in frames:
        overlay.add_trace(
            go.Scatter(
                x=frame["relative_minutes"],
                y=frame["glucose_mg_dL"],
                mode="lines",
                line=dict(width=1, color="rgba(0, 0, 255, 0.1)"),
                showlegend=False,
            )
        )
    overlay.update_layout(
        title=f"All windows | pattern={pattern_id}",
        xaxis_title="Minutes since window start",
        yaxis_title="Glucose (mg/dL)",
    )
    overlay.write_html(output_dir / f"{pattern_id}_overlay_minutes.html")

    hist = go.Figure()
    hist.add_trace(go.Histogram(x=combined["relative_minutes"], nbinsx=60, marker_color="rgba(0,0,255,0.5)"))
    hist.update_layout(
        title=f"Distribution of minutes | pattern={pattern_id}",
        xaxis_title="Minutes since window start",
        yaxis_title="Count",
    )
    hist.write_html(output_dir / f"{pattern_id}_hist_minutes.html")

    scatter = go.Figure(
        data=go.Scattergl(
            x=combined["relative_minutes"],
            y=combined["glucose_mg_dL"],
            mode="markers",
            marker=dict(size=3, opacity=0.15, color="blue"),
        )
    )
    scatter.update_layout(
        title=f"Scatter distribution | pattern={pattern_id}",
        xaxis_title="Minutes since window start",
        yaxis_title="Glucose (mg/dL)",
    )
    scatter.write_html(output_dir / f"{pattern_id}_scatter_minutes.html")


def cluster_windows(
    grid: np.ndarray,
    stacked: np.ndarray,
    metadata_df: pd.DataFrame,
    *,
    alignment: str,
    pattern_id: str,
    output_dir: Path,
    n_clusters: int,
) -> pd.DataFrame:
    clean = np.where(np.isnan(stacked), np.nanmedian(stacked, axis=0), stacked)
    clean = clean[:, ~np.all(np.isnan(clean), axis=0)]
    if clean.size == 0:
        print(f"All columns NaN for alignment={alignment}; skipping clustering.")
        return pd.DataFrame()

    col_mean = np.nanmean(clean, axis=0)
    clean = np.nan_to_num(clean, nan=col_mean)

    if clean.shape[0] < n_clusters or np.allclose(clean, clean[0]):
        print(f"Not enough variability for alignment={alignment}; skipping clustering.")
        return pd.DataFrame()

    labels = KMeans(n_clusters=n_clusters, random_state=42).fit_predict(clean)
    summary_rows = []

    for cluster_id in range(n_clusters):
        idxs = np.where(labels == cluster_id)[0]
        subset = stacked[idxs]
        if subset.size == 0:
            continue
        median, p10, p90, count = summarize_profile(subset)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=grid, y=median, mode="lines", name="Median"))
        fig.add_trace(go.Scatter(x=grid, y=p90, mode="lines", name="90th percentile", line=dict(dash="dash")))
        fig.add_trace(go.Scatter(x=grid, y=p10, mode="lines", name="10th percentile", line=dict(dash="dash")))
        fig.update_layout(
            title=f"Cluster {cluster_id + 1} (n={subset.shape[0]}) | alignment={alignment}",
            xaxis_title="Minutes",
            yaxis_title="Glucose (mg/dL)",
        )
        fig.write_html(output_dir / f"{pattern_id}_cluster_{alignment}_{cluster_id + 1}.html")

        row = {
            "alignment": alignment,
            "cluster": cluster_id + 1,
            "count": subset.shape[0],
        }
        if not metadata_df.empty:
            subset_meta = metadata_df.iloc[idxs]
            for column in ("minutes_low", "lowest_glucose"):
                if column in subset_meta and subset_meta[column].notna().any():
                    cleaned = subset_meta[column].dropna()
                    row[f"{column}_mean"] = cleaned.mean()
                    row[f"{column}_median"] = cleaned.median()
                    row[f"{column}_p25"] = cleaned.quantile(0.25)
                    row[f"{column}_p75"] = cleaned.quantile(0.75)

                    fig_dist = go.Figure()
                    fig_dist.add_trace(
                        go.Box(
                            y=cleaned,
                            name=f"Cluster {cluster_id + 1}",
                            boxmean="sd",
                        )
                    )
                    fig_dist.update_layout(
                        title=f"{column.replace('_', ' ').title()} distribution | alignment={alignment} | cluster {cluster_id + 1}",
                        yaxis_title=column.replace('_', ' ').title(),
                    )
                    fig_dist.write_html(
                        output_dir
                        / f"{pattern_id}_cluster_{alignment}_{cluster_id + 1}_{column}_distribution.html"
                    )
        summary_rows.append(row)

    return pd.DataFrame(summary_rows)


def analyze_pattern(
    pattern_id: str,
    detections_glob: str,
    *,
    output_dir: Path,
    patient_filter: str | None = None,
    max_examples: int | None = None,
    resample_minutes: int = 5,
    n_clusters: int = 3,
) -> pd.DataFrame:
    adapter = PATTERN_ADAPTERS.get(pattern_id)
    if adapter is None:
        raise ValueError(f"No adapter registered for pattern '{pattern_id}'.")

    examples = load_pattern_examples(pattern_id, detections_glob, patient_filter=patient_filter)
    if not examples:
        raise ValueError("No detection examples found for the specified parameters.")
    if max_examples is not None:
        examples = examples[:max_examples]

    window_frames: list[pd.DataFrame] = []
    metadata_rows: list[dict] = []
    start_hours: list[float] = []
    end_hours: list[float] = []

    for idx, entry in enumerate(examples, start=1):
        patient_id = entry["patient_id"]
        example = entry["example"]
        metrics = entry["metrics"]

        service_date = datetime.fromisoformat(example["service_date"]).replace(tzinfo=timezone.utc)
        day = fetch_day(patient_id, service_date)
        if day is None:
            continue

        frame = prepare_day_frame(day)
        window = adapter.slice_window(frame, example, metrics)
        if window.empty:
            continue

        start_hour = float(metrics.get(adapter.window_start_key, 0.0)) if adapter.window_start_key else 0.0
        end_hour = float(metrics.get(adapter.window_end_key, start_hour + 1.0)) if adapter.window_end_key else start_hour + 1.0

        window = add_relative_minutes(window, start_hour)
        window_frames.append(window[["relative_minutes", "glucose_mg_dL"]])
        metadata_rows.append(
            {
                "patient_id": patient_id,
                "service_date": example.get("service_date"),
                "minutes_low": example.get("minutes_low"),
                "lowest_glucose": example.get("lowest_glucose"),
            }
        )
        start_hours.append(start_hour)
        end_hours.append(end_hour)

        if idx % 50 == 0:
            print(f"Processed {idx} examples...")

    if not window_frames:
        raise ValueError("No windows met criteria after filtering.")

    output_dir.mkdir(parents=True, exist_ok=True)
    plot_overlay(window_frames, pattern_id, output_dir)

    metadata_df = pd.DataFrame(metadata_rows)
    summary_df = pd.DataFrame()

    for alignment in adapter.alignment_modes:
        if alignment == "nadir":
            grid, stacked = align_on_nadir(window_frames, resample_minutes)
        else:
            duration = float(np.mean(np.array(end_hours) - np.array(start_hours)))
            grid, stacked = align_on_window_start(window_frames, duration, resample_minutes)
        if grid is None or stacked is None:
            continue
        summary = cluster_windows(
            grid,
            stacked,
            metadata_df,
            alignment=alignment,
            pattern_id=pattern_id,
            output_dir=output_dir,
            n_clusters=n_clusters,
        )
        summary_df = pd.concat([summary_df, summary], ignore_index=True)

    if not summary_df.empty:
        summary_df.to_csv(output_dir / f"{pattern_id}_cluster_summary.csv", index=False)
    return summary_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyse CGM pattern detections and produce plots.")
    parser.add_argument("pattern_id", help="Pattern identifier (see PATTERN_ADAPTERS).")
    parser.add_argument("detections_glob", help="Glob for detection JSON files (use quotes).")
    parser.add_argument("output", help="Directory to write plots/summary.")
    parser.add_argument("--patient", help="Optional patient_id filter.")
    parser.add_argument("--max", dest="max_examples", type=int, help="Optional max examples to process.")
    parser.add_argument("--clusters", dest="n_clusters", type=int, default=3)
    parser.add_argument("--freq", dest="resample_minutes", type=int, default=5)
    args = parser.parse_args()

    summary = analyze_pattern(
        pattern_id=args.pattern_id,
        detections_glob=args.detections_glob,
        output_dir=Path(args.output),
        patient_filter=args.patient,
        max_examples=args.max_examples,
        resample_minutes=args.resample_minutes,
        n_clusters=args.n_clusters,
    )
    if summary.empty:
        print("No clustering summaries produced.")
    else:
        print(summary)


if __name__ == "__main__":
    main()

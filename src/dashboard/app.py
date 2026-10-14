"""Streamlit + Plotly dashboard (a QuickSight stand-in).

Tabs: Event Volume, Latency, Reliability, Freshness & Alarms, and Query
Performance. Filters run as SQL against the DuckDB warehouse. QuickSight-ready
assets live in ``dashboard/quicksight/``.

Run with::

    make dashboard
"""

from __future__ import annotations

import json

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from logwarehouse.config import get_settings
from monitoring.alarms import describe_alarm_states

SETTINGS = get_settings()
ARTIFACTS = SETTINGS.artifacts_dir

st.set_page_config(page_title="AWSLogEventWarehouse", layout="wide")


@st.cache_resource
def _connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(SETTINGS.duckdb_path), read_only=True)
    con.execute("SET TimeZone='UTC';")
    con.execute("USE warehouse;")
    return con


@st.cache_data(show_spinner=False)
def _query(sql: str) -> pd.DataFrame:
    return _connection().execute(sql).df()


def _load_json(name: str) -> dict:
    path = ARTIFACTS / name
    return json.loads(path.read_text()) if path.exists() else {}


def _date_bounds() -> tuple[pd.Timestamp, pd.Timestamp]:
    row = _query("SELECT min(dt) AS lo, max(dt) AS hi FROM fact_event_activity").iloc[0]
    return pd.Timestamp(row["lo"]), pd.Timestamp(row["hi"])


def sidebar_filters() -> tuple[list[pd.Timestamp], list[str]]:
    st.sidebar.header("Filters")
    lo, hi = _date_bounds()
    dates = st.sidebar.date_input("Event date range", value=(lo.date(), hi.date()))
    sources = st.sidebar.multiselect(
        "Sources", ["app_log", "clickstream"], default=["app_log", "clickstream"]
    )
    if isinstance(dates, tuple) and len(dates) == 2:
        return [pd.Timestamp(dates[0]), pd.Timestamp(dates[1])], sources or ["app_log"]
    return [lo, hi], sources or ["app_log"]


def _source_sql(sources: list[str]) -> str:
    joined = ",".join(f"'{s}'" for s in sources)
    return f"source IN ({joined})"


def tab_volume(dates: list[pd.Timestamp], sources: list[str]) -> None:
    st.subheader("Event volume by source / hour")
    df = _query(
        f"""
        SELECT dt, hour, source, count(*) AS events
        FROM fact_event_activity
        WHERE dt BETWEEN DATE '{dates[0].date()}' AND DATE '{dates[1].date()}'
          AND {_source_sql(sources)}
        GROUP BY dt, hour, source ORDER BY dt, hour
        """
    )
    if df.empty:
        st.info("No data for the selected filters.")
        return
    df["hour_ts"] = pd.to_datetime(df["dt"].astype(str)) + pd.to_timedelta(df["hour"], unit="h")
    fig = px.line(df, x="hour_ts", y="events", color="source", markers=True,
                  labels={"hour_ts": "hour", "events": "events"})
    st.plotly_chart(fig, use_container_width=True)
    total_cols = st.columns(len(sources))
    for col, src in zip(total_cols, sources, strict=False):
        col.metric(f"{src} events", f"{int(df[df['source'] == src]['events'].sum()):,}")


def tab_latency(dates: list[pd.Timestamp], sources: list[str]) -> None:
    st.subheader("Ingest-lag percentile trend")
    df = _query(
        f"""
        SELECT dt, hour,
               quantile_cont(ingest_lag_seconds, 0.5)  AS p50,
               quantile_cont(ingest_lag_seconds, 0.95) AS p95,
               quantile_cont(ingest_lag_seconds, 0.99) AS p99
        FROM fact_event_activity
        WHERE dt BETWEEN DATE '{dates[0].date()}' AND DATE '{dates[1].date()}'
          AND {_source_sql(sources)} AND ingest_lag_seconds IS NOT NULL
        GROUP BY dt, hour ORDER BY dt, hour
        """
    )
    if df.empty:
        st.info("No data for the selected filters.")
        return
    df["hour_ts"] = pd.to_datetime(df["dt"].astype(str)) + pd.to_timedelta(df["hour"], unit="h")
    fig = px.line(df, x="hour_ts", y=["p50", "p95", "p99"], markers=True,
                  labels={"value": "lag (s)", "hour_ts": "hour", "variable": "percentile"})
    st.plotly_chart(fig, use_container_width=True)
    snapshot = _load_json("metrics_snapshot.json")
    c1, c2, c3 = st.columns(3)
    c1.metric("p50 (s)", snapshot.get("ingest_lag_p50", "-"))
    c2.metric("p95 (s)", snapshot.get("ingest_lag_p95", "-"))
    c3.metric("p99 (s)", snapshot.get("ingest_lag_p99", "-"))


def tab_reliability(dates: list[pd.Timestamp], sources: list[str]) -> None:
    st.subheader("Reliability")
    by_service = _query(
        f"""
        SELECT s.service, avg(CASE WHEN f.is_error THEN 1.0 ELSE 0 END) * 100 AS error_rate
        FROM fact_event_activity f JOIN dim_service s USING (service_sk)
        WHERE f.dt BETWEEN DATE '{dates[0].date()}' AND DATE '{dates[1].date()}'
        GROUP BY s.service ORDER BY error_rate DESC
        """
    )
    df = _query(
        f"""
        SELECT dt, hour, avg(CASE WHEN is_error THEN 1.0 ELSE 0 END) * 100 AS error_rate
        FROM fact_event_activity
        WHERE dt BETWEEN DATE '{dates[0].date()}' AND DATE '{dates[1].date()}'
          AND {_source_sql(sources)}
        GROUP BY dt, hour ORDER BY dt, hour
        """
    )
    col1, col2 = st.columns(2)
    with col1:
        if not by_service.empty:
            st.plotly_chart(
                px.bar(by_service, x="service", y="error_rate", title="Error rate by service (%)"),
                use_container_width=True,
            )
    with col2:
        if not df.empty:
            df["hour_ts"] = pd.to_datetime(df["dt"].astype(str)) + pd.to_timedelta(
                df["hour"], unit="h"
            )
            st.plotly_chart(
                px.line(df, x="hour_ts", y="error_rate", title="Error rate over time (%)"),
                use_container_width=True,
            )
    snapshot = _load_json("metrics_snapshot.json")
    c1, c2, c3 = st.columns(3)
    c1.metric("Quarantine rate (%)", snapshot.get("quarantine_rate", "-"))
    c2.metric("Duplicate rate (%)", snapshot.get("duplicate_rate", "-"))
    c3.metric("Overall error rate (%)", snapshot.get("error_rate_overall", "-"))


def tab_freshness(dates: list[pd.Timestamp], sources: list[str]) -> None:
    st.subheader("Freshness & alarms")
    snapshot = _load_json("metrics_snapshot.json")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Freshness (min)", snapshot.get("freshness_minutes", "-"))
    c2.metric("Total events", f"{snapshot.get('total_events', 0):,}")
    c3.metric("Events last hour", f"{snapshot.get('events_last_hour', 0):,}")
    c4.metric("Last-hour drop (%)", snapshot.get("events_last_hour_drop_pct", "-"))

    states = describe_alarm_states(SETTINGS)
    if states:
        st.write("**Current CloudWatch alarm states**")
        st.dataframe(
            pd.DataFrame(
                [{"alarm": k, "state": v} for k, v in states.items()]
            ),
            use_container_width=True,
            hide_index=True,
        )
        if any(v == "ALARM" for v in states.values()):
            st.error("One or more pipeline alarms are in ALARM.")
        else:
            st.success("All pipeline alarms are OK.")
    else:
        st.info("No CloudWatch alarm states available (run `make monitor`).")

    evidence = _load_json("alarm_evidence.json")
    if evidence:
        st.write("**Last simulated incident**")
        st.write(f"Scenario: {evidence.get('scenario')} — fired: {', '.join(evidence.get('fired', []))}")
        st.dataframe(pd.DataFrame(evidence.get("alarms", [])), use_container_width=True,
                     hide_index=True)


def tab_performance(dates: list[pd.Timestamp], sources: list[str]) -> None:
    st.subheader("Query performance (before / after tuning)")
    bench = _load_json("query_benchmark.json")
    if not bench:
        st.info("Run `make bench` to populate the benchmark.")
        return
    rows = []
    for name, q in bench.get("queries", {}).items():
        rows.append(
            {
                "query": name,
                "baseline_median_s": q["baseline"]["median_seconds"],
                "tuned_median_s": q["tuned"]["median_seconds"],
                "speedup_x": q["speedup_median"],
                "rows_baseline": q["baseline"]["rows_scanned"],
                "rows_tuned": q["tuned"]["rows_scanned"],
            }
        )
    frame = pd.DataFrame(rows)
    st.dataframe(frame, use_container_width=True, hide_index=True)
    st.plotly_chart(
        px.bar(frame, x="query", y="speedup_x", title="Speedup factor (median runtime)"),
        use_container_width=True,
    )
    if (ARTIFACTS / "query_benchmark.md").exists():
        with st.expander("Benchmark table (markdown artifact)"):
            st.markdown((ARTIFACTS / "query_benchmark.md").read_text())


def main() -> None:
    """Render the dashboard."""
    st.title("AWSLogEventWarehouse")
    st.caption("Streaming log-event pipeline — LocalStack + PySpark + DuckDB")
    dates, sources = sidebar_filters()

    tabs = st.tabs(["Event Volume", "Latency", "Reliability", "Freshness & Alarms",
                    "Query Performance"])
    with tabs[0]:
        tab_volume(dates, sources)
    with tabs[1]:
        tab_latency(dates, sources)
    with tabs[2]:
        tab_reliability(dates, sources)
    with tabs[3]:
        tab_freshness(dates, sources)
    with tabs[4]:
        tab_performance(dates, sources)


main()

import marimo

__generated_with = "0.23.5"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import marimo as mo
    import altair as alt
    import polars as pl
    alt.data_transformers.enable("vegafusion")


@app.cell(hide_code=True)
def _(
    buffer_duration_sec,
    drop_fraction,
    recommended_limit,
    target_vcpu_capacity,
):
    mo.md(rf"""
    # OTLP Request Size Limit for Profiles

    This notebook estimates a good [default request size limit](https://github.com/open-telemetry/opentelemetry-proto/pull/782), after decompression, for OTLP servers and clients that support profiling.

    **tl;dr**: {recommended_limit / (1024 * 1024):.0f} MiB should be a good conservative limit for uncompressed profile requests from fully loaded {target_vcpu_capacity}-vCPU hosts using a {buffer_duration_sec}s buffer duration. Based on an analysis extrapolating from 10k real profiles, less than {drop_fraction*100:g}% of profiles would exceed the limit.

    For more details see:

    - [otlp-size-analysis by felixge · Pull Request #123 · open-telemetry/sig-profiling](https://github.com/open-telemetry/sig-profiling/pull/123)
    - [2026-05-06-10000.csv](https://drive.google.com/open?id=1bZV3L1YDq60meJCCPmeE0N-Pcg6kYezM&usp=drive_fs)

    ## Background

    Profiles use a [dictionary](https://github.com/open-telemetry/opentelemetry-proto/blob/1d70aa012dc42a5e74a215ce31c1fd84244ce89e/opentelemetry/proto/profiles/v1development/profiles.proto#L99-L177) to deduplicate data such as stack traces, file names, and function names. The graph below shows a simplified model of how this leads to efficiency gains as more samples are buffered for longer durations. It assumes a steady sample rate and that each sample captures a timestamp.
    """)
    return


@app.cell(hide_code=True)
def _():
    _time_size_curve_data = (
        pl.DataFrame({"time": list(range(0, 101))})
        .with_columns(
            (24 * (1 + pl.col("time")).log()).alias("dictionary_size"),
            (1.8 * pl.col("time")).alias("sample_size"),
            (24 / (1 + pl.col("time"))).alias("dictionary_data_rate"),
            pl.lit(1.8).alias("sample_data_rate"),
        )
        .with_columns(
            (pl.col("dictionary_size") + pl.col("sample_size")).alias("size"),
            (pl.col("dictionary_data_rate") + pl.col("sample_data_rate")).alias("data_rate"),
            pl.when(pl.col("time") <= 50)
            .then(pl.lit("First half: dictionary size characteristic dominates"))
            .otherwise(pl.lit("Second half: sample size characteristic dominates"))
            .alias("region"),
        )
    )

    _time_size_total_data = _time_size_curve_data.with_columns(
        pl.lit("Total Size").alias("component_label")
    )

    _time_size_total_line = (
        alt.Chart(_time_size_total_data)
        .mark_line(
            strokeWidth=4,
            interpolate="basis",
            strokeCap="round",
            strokeJoin="round",
        )
        .encode(
            x=alt.X("time:Q", title="Buffer Duration", axis=alt.Axis(labels=False)),
            y=alt.Y("size:Q", title="Size", axis=alt.Axis(labels=False)),
            color=alt.Color(
                "component_label:N",
                title="Component",
                scale=alt.Scale(
                    domain=["Total Size", "Dictionary Size", "Sample Size"],
                    range=["#2F4B7C", "#54A24B", "#E45756"],
                ),
            ),
        )
    )

    _time_size_total_line_shadow = (
        alt.Chart(_time_size_total_data)
        .mark_line(
            strokeWidth=2,
            opacity=0.45,
            interpolate="basis",
            strokeCap="round",
            strokeJoin="round",
        )
        .encode(
            x=alt.X("time:Q", title="Buffer Duration", axis=alt.Axis(labels=False)),
            y=alt.Y(
                "size:Q",
                title="Size",
                axis=alt.Axis(labels=False),
            ),
            color=alt.Color(
                "component_label:N",
                title="Component",
                scale=alt.Scale(
                    domain=["Total Size", "Dictionary Size", "Sample Size"],
                    range=["#2F4B7C", "#54A24B", "#E45756"],
                ),
            ),
        )
    )

    _time_size_component_data = (
        _time_size_curve_data.unpivot(
            index="time",
            on=["dictionary_size", "sample_size"],
            variable_name="component",
            value_name="component_size",
        ).with_columns(
            pl.when(pl.col("component") == "dictionary_size")
            .then(pl.lit("Dictionary Size"))
            .otherwise(pl.lit("Sample Size"))
            .alias("component_label")
        )
    )

    _time_size_component_lines = (
        alt.Chart(_time_size_component_data)
        .mark_line(
            strokeDash=[8, 5],
            strokeWidth=2.5,
            opacity=0.8,
            interpolate="basis",
            strokeCap="round",
            strokeJoin="round",
        )
        .encode(
            x=alt.X("time:Q", title="Buffer Duration", axis=alt.Axis(labels=False)),
            y=alt.Y(
                "component_size:Q",
                title="Size",
                axis=alt.Axis(labels=False),
            ),
            color=alt.Color(
                "component_label:N",
                title="Component",
                scale=alt.Scale(
                    domain=["Total Size", "Dictionary Size", "Sample Size"],
                    range=["#2F4B7C", "#54A24B", "#E45756"],
                ),
            ),
        )
    )

    _profile_size_growth_panel = (
        alt.layer(
            _time_size_component_lines,
            _time_size_total_line_shadow,
            _time_size_total_line,
        )
        .properties(
            title="Profile Size vs Buffer Duration",
            width=290,
            height=200,
        )
    )

    _time_size_rate_total_data = _time_size_curve_data.with_columns(
        pl.lit("Total Data Rate").alias("component_label")
    )

    _time_size_rate_line = (
        alt.Chart(_time_size_rate_total_data)
        .mark_line(
            strokeWidth=4,
            interpolate="basis",
            strokeCap="round",
            strokeJoin="round",
        )
        .encode(
            x=alt.X("time:Q", title="Buffer Duration", axis=alt.Axis(labels=False)),
            y=alt.Y("data_rate:Q", title="Data Rate", axis=alt.Axis(labels=False)),
            color=alt.Color(
                "component_label:N",
                title="Component",
                scale=alt.Scale(
                    domain=["Total Data Rate", "Dictionary Data Rate", "Sample Data Rate"],
                    range=["#2F4B7C", "#54A24B", "#E45756"],
                ),
            ),
        )
    )

    _time_size_rate_component_data = (
        _time_size_curve_data.unpivot(
            index="time",
            on=["dictionary_data_rate", "sample_data_rate"],
            variable_name="component",
            value_name="component_data_rate",
        ).with_columns(
            pl.when(pl.col("component") == "dictionary_data_rate")
            .then(pl.lit("Dictionary Data Rate"))
            .otherwise(pl.lit("Sample Data Rate"))
            .alias("component_label")
        )
    )

    _time_size_rate_component_lines = (
        alt.Chart(_time_size_rate_component_data)
        .mark_line(
            strokeDash=[8, 5],
            strokeWidth=2.5,
            opacity=0.8,
            interpolate="basis",
            strokeCap="round",
            strokeJoin="round",
        )
        .encode(
            x=alt.X("time:Q", title="Buffer Duration", axis=alt.Axis(labels=False)),
            y=alt.Y(
                "component_data_rate:Q",
                title="Data Rate",
                axis=alt.Axis(labels=False),
            ),
            color=alt.Color(
                "component_label:N",
                title="Component",
                scale=alt.Scale(
                    domain=["Total Data Rate", "Dictionary Data Rate", "Sample Data Rate"],
                    range=["#2F4B7C", "#54A24B", "#E45756"],
                ),
            ),
        )
    )

    _profile_data_rate_panel = (
        alt.layer(_time_size_rate_component_lines, _time_size_rate_line)
        .properties(
            title="Profile Data Rate vs Buffer Duration",
            width=290,
            height=200,
        )
    )

    time_size_log_linear_chart = (
        alt.hconcat(_profile_size_growth_panel, _profile_data_rate_panel)
        .resolve_scale(y="independent", color="independent")
        .configure_axis(
            grid=False,
            labelFont="Comic Sans MS, Comic Sans, cursive",
            titleFont="Comic Sans MS, Comic Sans, cursive",
            titleFontSize=16,
            domainWidth=2,
            tickWidth=2,
        )
        .configure_title(
            font="Comic Sans MS, Comic Sans, cursive",
            fontSize=18,
            anchor="start",
        )
        .configure_legend(
            labelFont="Comic Sans MS, Comic Sans, cursive",
            labelFontSize=16,
            titleFont="Comic Sans MS, Comic Sans, cursive",
            titleFontSize=16,
        )
        .configure_view(strokeWidth=0)
    )

    time_size_log_linear_chart
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    *Note: For profiles that don't capture timestamps, the `Sample Size` slope would also decrease over time, and `Total Size` would eventually flatten out.*
    """)
    return


@app.cell(hide_code=True)
def _(dataset_name, df, target_vcpu_capacity):
    mo.md(rf"""
    ## Assumptions

    To estimate a good limit for profile requests, we need to make some assumptions:

    * We focus on CPU profiles because they are the most common profile type and are supported by the [opentelemetry-ebpf-profiler](https://github.com/open-telemetry/opentelemetry-ebpf-profiler).
    * Profiles from heavily loaded hosts, such as hot shards, are especially interesting. We should aim to drop less than 1% of them.
    * We use {target_vcpu_capacity} vCPUs as a conservative upper bound for the default limit. Based on internal estimates, we think fewer than 5% of customers use hosts with more vCPUs in production. Users with larger hosts can configure another limit or buffer duration.
    * For better efficiency, the eBPF profiler should switch to a ~60s (1) default buffer duration instead of the current [5s](https://github.com/open-telemetry/opentelemetry-ebpf-profiler/blob/2e28aeaeb3a94b018e5db131550a78627bd056cf/collector/factory.go#L32).

    *(1) The Profiling SIG will need to spend more time discussing whether 60s is the Goldilocks zone. It's possible that even longer buffer periods should be considered, but at some point they become a latency issue. We picked 60s because that's what we use for our SDK profilers at Datadog. However, it should be noted that those use 100 CPU samples per second, which causes dictionaries to amortize 5x faster.*

    ## Dataset

    We also need to look at real data. For this purpose, we share [{dataset_name}](https://drive.google.com/open?id=1bZV3L1YDq60meJCCPmeE0N-Pcg6kYezM&usp=drive_fs), which contains statistics from a set of profiles:

    * The data is from the eBPF profiler using 20 samples per second ([SamplesPerSecond](https://github.com/open-telemetry/opentelemetry-ebpf-profiler/blob/2e28aeaeb3a94b018e5db131550a78627bd056cf/collector/factory.go#L35)) and a non-default 60s buffer duration ([ReporterInterval](https://github.com/open-telemetry/opentelemetry-ebpf-profiler/blob/2e28aeaeb3a94b018e5db131550a78627bd056cf/collector/factory.go#L32)).
    * It originates from a staging environment at Datadog where Go, Java, and Native (C, C++, Rust) are the most dominant languages.
    * Go and Native samples do not contain symbols (e.g., function names); they are uploaded out of band.
    * The dataset contains {len(df):,} randomly selected OTLP profiles from {df["host_id"].n_unique():,} unique hosts collected over a one-week period.
    * The CSV was produced by running he [otlp-analyze](https://github.com/open-telemetry/sig-profiling/pull/123) program against the OTLP payloads. Feel free to send PRs if you are interested in seeing more columns.
    """)
    return


@app.cell
def _():
    drop_fraction = 1 / 100

    target_vcpu_capacity = 64
    buffer_duration_sec = 60
    samples_per_second = 20
    return (
        buffer_duration_sec,
        drop_fraction,
        samples_per_second,
        target_vcpu_capacity,
    )


@app.cell(hide_code=True)
def _(samples_per_second):
    dataset_name = "2026-05-06-10000.csv"
    df = pl.read_csv("data/"+dataset_name, infer_schema_length=None)
    _language_fraction_columns = [
        column_name
        for column_name in df.columns
        if column_name.endswith("_fraction")
        and not column_name.endswith("_symbol_fraction")
    ]
    df = (
        df.with_columns(
            (pl.col("uncompressed_bytes") / (1024 * 1024)).alias("uncompressed_mib"),
            (pl.col("uncompressed_bytes") / pl.col("samples_count")).alias("mean_bytes_per_sample"),
            (pl.col("samples_count") / samples_per_second / pl.col("duration_sec")).alias("mean_cpu_usage_cores"),
            (pl.col("resource_profiles_attrs_count") / pl.col("resource_profiles_count")).alias("mean_attrs_per_resource"),
            (pl.col("samples_count") / pl.col("dict_stacks_count")).alias("mean_samples_per_stack"),
            pl.struct(_language_fraction_columns)
            .map_elements(
                lambda fractions: max(
                    _language_fraction_columns,
                    key=lambda column_name: fractions[column_name]
                    if fractions[column_name] is not None
                    else float("-inf"),
                ).removesuffix("_fraction"),
                return_dtype=pl.String,
            )
            .alias("primary_language"),
        )
        # .filter(pl.col("mean_attrs_per_resource") >= 10)
    )
    df
    return dataset_name, df


@app.cell(hide_code=True)
def _(df, drop_fraction):
    mo.md(rf"""
    The graph below shows the distribution of uncompressed profile sizes in the dataset. A naive conclusion would be that a limit above the p{100-drop_fraction*100:g} profile size ({df['uncompressed_mib'].quantile(0.99):.2f} MiB) should be sufficient.
    """)
    return


@app.cell(hide_code=True)
def _(df):
    _uncompressed_mib_summary = (
        df.select(
            pl.col("uncompressed_mib").median().alias("median"),
            pl.col("uncompressed_mib").mean().alias("mean"),
            pl.col("uncompressed_mib").quantile(0.99).alias("q99"),
            pl.col("uncompressed_mib").quantile(0.999).alias("q99_9"),
            pl.col("uncompressed_mib").max().alias("max"),
        )
        .row(0, named=True)
    )
    _uncompressed_mib_reference_lines = pl.DataFrame(
        {
            "statistic": ["Median", "Mean", "p99", "p99.9", "Max"],
            "value": [
                _uncompressed_mib_summary["median"],
                _uncompressed_mib_summary["mean"],
                _uncompressed_mib_summary["q99"],
                _uncompressed_mib_summary["q99_9"],
                _uncompressed_mib_summary["max"],
            ],
            "label": [
                f"Median: {_uncompressed_mib_summary['median']:,.2f} MiB",
                f"Mean: {_uncompressed_mib_summary['mean']:,.2f} MiB",
                f"p99: {_uncompressed_mib_summary['q99']:,.2f} MiB",
                f"p99.9: {_uncompressed_mib_summary['q99_9']:,.2f} MiB",
                f"Max: {_uncompressed_mib_summary['max']:,.2f} MiB",
            ],
        }
    )

    _uncompressed_mib_histogram_bars = (
        alt.Chart(df)
        .mark_bar(color="#4C78A8")
        .encode(
            x=alt.X(
                "uncompressed_mib:Q",
                bin=alt.Bin(maxbins=40),
                title="Uncompressed profile size (MiB)",
            ),
            y=alt.Y("count():Q", title="Count"),
            tooltip=[
                alt.Tooltip("count():Q", title="Count"),
            ],
        )
    )

    _uncompressed_mib_reference_rules = (
        alt.Chart(_uncompressed_mib_reference_lines)
        .mark_rule(strokeWidth=2)
        .encode(
            x=alt.X("value:Q", title="Uncompressed profile size (MiB)"),
            color=alt.Color(
                "label:N",
                title="Statistic",
                scale=alt.Scale(scheme="category10"),
            ),
            tooltip=[
                alt.Tooltip("statistic:N", title="Statistic"),
                alt.Tooltip("value:Q", title="Value (MiB)", format=",.2f"),
            ],
        )
    )

    uncompressed_bytes_histogram = (
        alt.layer(
            _uncompressed_mib_histogram_bars,
            _uncompressed_mib_reference_rules,
        )
        .properties(
            title="Histogram of uncompressed profile size",
            width=700,
            height=400,
        )
        .interactive()
    )

    uncompressed_bytes_histogram
    return


@app.cell(hide_code=True)
def _(df, target_vcpu_capacity):
    mo.md(rf"""
    The problem with the naive approach is that our dataset doesn't have any profiles from fully loaded {target_vcpu_capacity}-vCPU hosts (see Assumptions). In fact, we only have a single profile where inferred average CPU usage reached {df["mean_cpu_usage_cores"].max():.1f} active cores.
    """)
    return


@app.cell(hide_code=True)
def _(df):
    _mean_cpu_usage_vs_uncompressed_size_data = df.select(
        "uncompressed_mib",
        "mean_cpu_usage_cores",
        "mean_bytes_per_sample",
        "mean_attrs_per_resource",
        "samples_count",
        "duration_sec",
        "primary_language",
    )

    _mean_cpu_usage_vs_uncompressed_size_stats = _mean_cpu_usage_vs_uncompressed_size_data.select(
        pl.col("uncompressed_mib").mean().alias("x_mean"),
        pl.col("mean_cpu_usage_cores").mean().alias("y_mean"),
        pl.col("uncompressed_mib").min().alias("x_min"),
        pl.col("uncompressed_mib").max().alias("x_max"),
    ).row(0, named=True)

    _mean_cpu_usage_vs_uncompressed_size_slope = _mean_cpu_usage_vs_uncompressed_size_data.select(
        (
            (
                (pl.col("uncompressed_mib") - _mean_cpu_usage_vs_uncompressed_size_stats["x_mean"])
                * (pl.col("mean_cpu_usage_cores") - _mean_cpu_usage_vs_uncompressed_size_stats["y_mean"])
            ).sum()
            / (
                (pl.col("uncompressed_mib") - _mean_cpu_usage_vs_uncompressed_size_stats["x_mean"])
                ** 2
            ).sum()
        ).alias("slope")
    ).item()

    _mean_cpu_usage_vs_uncompressed_size_intercept = (
        _mean_cpu_usage_vs_uncompressed_size_stats["y_mean"]
        - _mean_cpu_usage_vs_uncompressed_size_slope
        * _mean_cpu_usage_vs_uncompressed_size_stats["x_mean"]
    )

    _mean_cpu_usage_vs_uncompressed_size_regression_data = pl.DataFrame(
        {
            "uncompressed_mib": [
                _mean_cpu_usage_vs_uncompressed_size_stats["x_min"],
                _mean_cpu_usage_vs_uncompressed_size_stats["x_max"],
            ],
        }
    ).with_columns(
        (
            _mean_cpu_usage_vs_uncompressed_size_intercept
            + _mean_cpu_usage_vs_uncompressed_size_slope * pl.col("uncompressed_mib")
        ).alias("mean_cpu_usage_cores"),
    )

    _mean_cpu_usage_vs_uncompressed_size_base = alt.Chart(
        _mean_cpu_usage_vs_uncompressed_size_data
    ).encode(
        x=alt.X(
            "uncompressed_mib:Q",
            title="Uncompressed profile size (MiB)",
            scale=alt.Scale(zero=False),
        ),
        y=alt.Y(
            "mean_cpu_usage_cores:Q",
            title="Mean CPU usage (cores)",
            scale=alt.Scale(zero=False),
        ),
    )

    _mean_cpu_usage_vs_uncompressed_size_points = (
        _mean_cpu_usage_vs_uncompressed_size_base.mark_circle(
            size=45,
            opacity=0.55,
        ).encode(
            color=alt.Color(
                "primary_language:N",
                title="Primary language",
                scale=alt.Scale(scheme="category10"),
            ),
            tooltip=[
                alt.Tooltip("uncompressed_mib:Q", title="Uncompressed size (MiB)", format=",.2f"),
                alt.Tooltip("mean_cpu_usage_cores:Q", title="Mean CPU usage (cores)", format=",.2f"),
                alt.Tooltip("primary_language:N", title="Primary language"),
                alt.Tooltip("mean_bytes_per_sample:Q", title="Mean bytes per sample", format=",.2f"),
                alt.Tooltip("mean_attrs_per_resource:Q", title="Mean attrs per resource", format=",.2f"),
                alt.Tooltip("samples_count:Q", title="Samples count", format=",.0f"),
                alt.Tooltip("duration_sec:Q", title="Duration seconds", format=",.2f"),
            ],
        )
    )

    _mean_cpu_usage_vs_uncompressed_size_regression_line = (
        alt.Chart(_mean_cpu_usage_vs_uncompressed_size_regression_data)
        .mark_line(strokeWidth=3, color="#2F4B7C")
        .encode(
            x=alt.X("uncompressed_mib:Q", title="Uncompressed profile size (MiB)"),
            y=alt.Y("mean_cpu_usage_cores:Q", title="Mean CPU usage (cores)"),
            tooltip=[
                alt.Tooltip("uncompressed_mib:Q", title="Uncompressed size (MiB)", format=",.2f"),
                alt.Tooltip("mean_cpu_usage_cores:Q", title="Fitted mean CPU usage (cores)", format=",.2f"),
            ],
        )
    )

    mean_cpu_usage_vs_uncompressed_size_point_cloud = (
        alt.layer(
            _mean_cpu_usage_vs_uncompressed_size_points,
            _mean_cpu_usage_vs_uncompressed_size_regression_line,
        )
        .properties(
            title="Mean CPU usage vs. uncompressed profile size",
            width=700,
            height=450,
        )
        .interactive()
    )

    mean_cpu_usage_vs_uncompressed_size_point_cloud
    return


@app.cell(hide_code=True)
def _(target_vcpu_capacity):
    mo.md(rf"""
    However, we can upscale each profile by calculating the `target_samples` that would be produced by a fully loaded host with {target_vcpu_capacity} vCPUs and multiplying that by the profile's `mean_bytes_per_sample`.

    From this, we can determine the uncompressed `size_limit` in MiB.
    """)
    return


@app.cell
def _(
    buffer_duration_sec,
    df,
    drop_fraction,
    samples_per_second,
    target_vcpu_capacity,
):
    target_samples = target_vcpu_capacity * buffer_duration_sec * samples_per_second
    upscaled_profile_sizes = df["mean_bytes_per_sample"] * target_samples
    size_limit = upscaled_profile_sizes.quantile(1 - drop_fraction)
    size_limit / (1024 * 1024)
    return size_limit, upscaled_profile_sizes


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    **Important:** The `size_limit` above is expected to be an overestimate because profiles with lower average CPU usage have a higher `mean_bytes_per_sample` and they are overrepresented in the dataset we are using.

    In other words, this is intended to be a conservative estimate. Now that we have a limit, let's round it up to the nearest power of 2 for additional margin, aesthetic purposes, and acknowledgment of the uncertainty of the estimate.
    """)
    return


@app.cell
def _(size_limit):
    recommended_limit = 2 ** (int(size_limit) - 1).bit_length()
    recommended_limit / (1024 * 1024)
    return (recommended_limit,)


@app.cell(hide_code=True)
def _(target_vcpu_capacity):
    mo.md(rf"""
    For a quick sanity check, the histogram below shows how our `recommended_limit` falls within the upscaled distribution of uncompressed profile sizes we have derived.

    If we had a real distribution of profiles from fully loaded {target_vcpu_capacity}-vCPU hosts, we would expect them to have a lower mean and lower standard deviation, resulting in an even lower drop rate.
    """)
    return


@app.cell(hide_code=True)
def _(
    buffer_duration_sec,
    recommended_limit,
    target_vcpu_capacity,
    upscaled_profile_sizes,
):
    _upscaled_profile_sizes_data = pl.DataFrame(
        {"upscaled_profile_size_bytes": upscaled_profile_sizes}
    ).with_columns(
        (pl.col("upscaled_profile_size_bytes") / (1024 * 1024)).alias(
            "upscaled_profile_size_mib"
        )
    )

    _upscaled_profile_sizes_summary = (
        _upscaled_profile_sizes_data.select(
            pl.col("upscaled_profile_size_mib").median().alias("median"),
            pl.col("upscaled_profile_size_mib").mean().alias("mean"),
            pl.col("upscaled_profile_size_mib").quantile(0.99).alias("p99"),
            pl.col("upscaled_profile_size_mib").quantile(0.999).alias("p99_9"),
            pl.col("upscaled_profile_size_mib").max().alias("max"),
            pl.lit(recommended_limit / (1024 * 1024)).alias("recommended_limit"),
        )
        .row(0, named=True)
    )

    _upscaled_profile_sizes_reference_lines = pl.DataFrame(
        {
            "statistic": [
                "Median",
                "Mean",
                "p99",
                "Max",
                "Recommended limit",
            ],
            "value": [
                _upscaled_profile_sizes_summary["median"],
                _upscaled_profile_sizes_summary["mean"],
                _upscaled_profile_sizes_summary["p99"],
                _upscaled_profile_sizes_summary["max"],
                _upscaled_profile_sizes_summary["recommended_limit"],
            ],
            "label": [
                f"Median: {_upscaled_profile_sizes_summary['median']:,.2f} MiB",
                f"Mean: {_upscaled_profile_sizes_summary['mean']:,.2f} MiB",
                f"p99: {_upscaled_profile_sizes_summary['p99']:,.2f} MiB",
                f"Max: {_upscaled_profile_sizes_summary['max']:,.2f} MiB",
                f"Recommended limit: {_upscaled_profile_sizes_summary['recommended_limit']:,.0f} MiB",
            ],
        }
    )

    _upscaled_profile_sizes_reference_color = alt.Color(
        "label:N",
        title="Statistic",
        scale=alt.Scale(scheme="category10"),
    )

    _upscaled_profile_sizes_bars = (
        alt.Chart(_upscaled_profile_sizes_data)
        .mark_bar(color="#4C78A8")
        .encode(
            x=alt.X(
                "upscaled_profile_size_mib:Q",
                bin=alt.Bin(maxbins=50),
                title="Upscaled profile size (MiB)",
            ),
            y=alt.Y("count():Q", title="Profile count"),
            tooltip=[
                alt.Tooltip("count():Q", title="Profile count"),
            ],
        )
    )

    _upscaled_profile_sizes_reference_rules = (
        alt.Chart(_upscaled_profile_sizes_reference_lines)
        .mark_rule(strokeWidth=2)
        .encode(
            x=alt.X("value:Q", title="Upscaled profile size (MiB)"),
            color=_upscaled_profile_sizes_reference_color,
            tooltip=[
                alt.Tooltip("statistic:N", title="Statistic"),
                alt.Tooltip("value:Q", title="Size (MiB)", format=",.2f"),
            ],
        )
    )

    _upscaled_profile_sizes_histogram_selection = alt.selection_interval(
        bind="scales",
        name="upscaled_profile_sizes_histogram_zoom",
    )

    upscaled_profile_sizes_histogram = (
        alt.layer(
            _upscaled_profile_sizes_bars,
            _upscaled_profile_sizes_reference_rules,
        )
        .properties(
            title=f"Histogram of profile sizes upscaled to fully loaded {target_vcpu_capacity}-vCPU hosts over {buffer_duration_sec}s",
            width=344,
            height=400,
        )
        .add_params(_upscaled_profile_sizes_histogram_selection)
    )

    _upscaled_profile_size_percentiles_data = pl.DataFrame(
        {
            "percentile": list(range(101)),
            "upscaled_profile_size_mib": [
                upscaled_profile_sizes.quantile(percentile / 100) / (1024 * 1024)
                for percentile in range(101)
            ],
        }
    )

    _upscaled_profile_size_percentile_line = (
        alt.Chart(_upscaled_profile_size_percentiles_data)
        .mark_line(strokeWidth=3, color="#F58518")
        .encode(
            x=alt.X(
                "upscaled_profile_size_mib:Q",
                title="Upscaled profile size (MiB)",
                scale=alt.Scale(zero=False),
            ),
            y=alt.Y(
                "percentile:Q",
                title="Percentile",
                scale=alt.Scale(domain=[0, 100]),
                axis=alt.Axis(format=".0f"),
            ),
            tooltip=[
                alt.Tooltip("percentile:Q", title="Percentile", format=".0f"),
                alt.Tooltip(
                    "upscaled_profile_size_mib:Q",
                    title="Upscaled profile size (MiB)",
                    format=",.2f",
                ),
            ],
        )
    )

    _upscaled_profile_size_percentile_recommended_rule = (
        alt.Chart(_upscaled_profile_sizes_reference_lines.filter(pl.col("statistic") == "Recommended limit"))
        .mark_rule(strokeWidth=2)
        .encode(
            x=alt.X("value:Q"),
            color=_upscaled_profile_sizes_reference_color,
            tooltip=[
                alt.Tooltip("label:N", title="Reference"),
                alt.Tooltip("value:Q", title="Size (MiB)", format=",.2f"),
            ],
        )
    )

    _upscaled_profile_size_percentile_selection = alt.selection_interval(
        bind="scales",
        name="upscaled_profile_size_percentile_zoom",
    )

    upscaled_profile_sizes_percentile_chart = (
        alt.layer(
            _upscaled_profile_size_percentile_line,
            _upscaled_profile_size_percentile_recommended_rule,
        )
        .properties(
            title=f"Percentiles of profile sizes upscaled to fully loaded {target_vcpu_capacity}-vCPU hosts over {buffer_duration_sec}s",
            width=344,
            height=400,
        )
        .add_params(_upscaled_profile_size_percentile_selection)
    )

    upscaled_profile_sizes_distribution_charts = alt.hconcat(
        upscaled_profile_sizes_histogram,
        upscaled_profile_sizes_percentile_chart,
    ).resolve_scale(y="independent")

    upscaled_profile_sizes_distribution_charts
    return


@app.cell(hide_code=True)
def _(
    buffer_duration_sec,
    drop_fraction,
    recommended_limit,
    target_vcpu_capacity,
):
    mo.md(rf"""
    So in conclusion, {recommended_limit / (1024 * 1024):.0f} MiB should be a good conservative limit for uncompressed profile requests from fully loaded hosts with up to {target_vcpu_capacity} vCPUs using a {buffer_duration_sec}s buffer duration. Based on this extrapolation, less than {drop_fraction*100:g}% of profiles would exceed the limit.

    This estimate is based on CPU profiles, but profiling support is expanding beyond CPU data. Memory profiles and other profile types may have different size characteristics, and profile payloads may also grow as the eBPF profiler gains more features or as OpenTelemetry SDK profilers are released and adopted. Because the limit above is intentionally conservative, it should leave some headroom for these future use cases without immediately requiring another increase.

    Whether or not this value is acceptable for other purposes, e.g. managing memory management in the collector, is not being analyzed here.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Data Exploration
    """)
    return


@app.cell(hide_code=True)
def _(df):
    _language_fraction_stack_frame_columns = [
        column_name
        for column_name in df.columns
        if column_name.endswith("_fraction")
        and not column_name.endswith("_symbol_fraction")
    ]

    _language_fraction_stack_frame_data = (
        df.select(
            [
                (
                    pl.col(column_name).cast(pl.Float64, strict=False).fill_null(0)
                    * pl.col("sampled_stack_frames_count")
                )
                .sum()
                .alias(column_name.removesuffix("_fraction"))
                for column_name in _language_fraction_stack_frame_columns
            ]
        )
        .unpivot(variable_name="language", value_name="stack_frame_count")
        .with_columns(
            (
                100
                * pl.col("stack_frame_count")
                / pl.col("stack_frame_count").sum()
            ).alias("stack_frame_percentage")
        )
        .sort("stack_frame_percentage", descending=True)
    )

    _language_fraction_stack_frame_bars = (
        alt.Chart(_language_fraction_stack_frame_data)
        .mark_bar(color="#4C78A8")
        .encode(
            x=alt.X(
                "stack_frame_percentage:Q",
                title="Percentage of stack frames",
                axis=alt.Axis(format=".0f"),
            ),
            y=alt.Y("language:N", title="Language", sort="-x"),
            tooltip=[
                alt.Tooltip("language:N", title="Language"),
                alt.Tooltip(
                    "stack_frame_percentage:Q",
                    title="Stack frames",
                    format=".2f",
                ),
                alt.Tooltip(
                    "stack_frame_count:Q",
                    title="Estimated stack frame count",
                    format=",.0f",
                ),
            ],
        )
    )

    _language_fraction_stack_frame_labels = (
        alt.Chart(_language_fraction_stack_frame_data)
        .mark_text(align="left", baseline="middle", dx=3)
        .encode(
            x=alt.X("stack_frame_percentage:Q"),
            y=alt.Y("language:N", sort="-x"),
            text=alt.Text("stack_frame_percentage:Q", format=".1f"),
        )
    )

    language_fraction_stack_frame_percentage_bar_chart = (
        alt.layer(
            _language_fraction_stack_frame_bars,
            _language_fraction_stack_frame_labels,
        )
        .properties(
            title="Percentage of stack frames by language fraction",
            width=700,
            height=350,
        )
        .interactive()
    )

    language_fraction_stack_frame_percentage_bar_chart
    return


@app.cell(hide_code=True)
def _(df):
    _language_symbol_fraction_stack_frame_columns = [
        column_name
        for column_name in df.columns
        if column_name.endswith("_symbol_fraction")
    ]

    if _language_symbol_fraction_stack_frame_columns:
        _language_symbol_fraction_stack_frame_data = (
            pl.concat(
                [
                    df.select(
                        pl.lit(language).alias("language"),
                        (
                            pl.col(f"{language}_fraction")
                            .cast(pl.Float64, strict=False)
                            .fill_null(0)
                            * pl.col("sampled_stack_frames_count")
                        )
                        .sum()
                        .alias("stack_frame_count"),
                        (
                            pl.col(f"{language}_symbol_fraction")
                            .cast(pl.Float64, strict=False)
                            .fill_null(0)
                            * pl.col(f"{language}_fraction")
                            .cast(pl.Float64, strict=False)
                            .fill_null(0)
                            * pl.col("sampled_stack_frames_count")
                        )
                        .sum()
                        .alias("symbolized_stack_frame_count"),
                    )
                    for language in [
                        column_name.removesuffix("_symbol_fraction")
                        for column_name in _language_symbol_fraction_stack_frame_columns
                    ]
                    if f"{language}_fraction" in df.columns
                ]
            )
            .filter(pl.col("stack_frame_count") > 0)
            .with_columns(
                (
                    100
                    * pl.col("symbolized_stack_frame_count")
                    / pl.col("stack_frame_count")
                ).alias("symbolized_stack_frame_percentage")
            )
            .sort("symbolized_stack_frame_percentage", descending=True)
        )

        _language_symbol_fraction_stack_frame_bars = (
            alt.Chart(_language_symbol_fraction_stack_frame_data)
            .mark_bar(color="#54A24B")
            .encode(
                x=alt.X(
                    "symbolized_stack_frame_percentage:Q",
                    title="Percentage of frames symbolized",
                    axis=alt.Axis(format=".0f"),
                ),
                y=alt.Y("language:N", title="Language", sort="-x"),
                tooltip=[
                    alt.Tooltip("language:N", title="Language"),
                    alt.Tooltip(
                        "symbolized_stack_frame_percentage:Q",
                        title="Frames symbolized",
                        format=".2f",
                    ),
                    alt.Tooltip(
                        "symbolized_stack_frame_count:Q",
                        title="Estimated symbolized stack frame count",
                        format=",.0f",
                    ),
                    alt.Tooltip(
                        "stack_frame_count:Q",
                        title="Estimated stack frame count",
                        format=",.0f",
                    ),
                ],
            )
        )

        _language_symbol_fraction_stack_frame_labels = (
            alt.Chart(_language_symbol_fraction_stack_frame_data)
            .mark_text(align="left", baseline="middle", dx=3)
            .encode(
                x=alt.X("symbolized_stack_frame_percentage:Q"),
                y=alt.Y("language:N", sort="-x"),
                text=alt.Text("symbolized_stack_frame_percentage:Q", format=".1f"),
            )
        )

        language_symbol_fraction_stack_frame_percentage_bar_chart = (
            alt.layer(
                _language_symbol_fraction_stack_frame_bars,
                _language_symbol_fraction_stack_frame_labels,
            )
            .properties(
                title="Percentage of frames symbolized by language",
                width=700,
                height=350,
            )
            .interactive()
        )
    else:
        language_symbol_fraction_stack_frame_percentage_bar_chart = mo.md(
            "No `LANG_symbol_fraction` columns were found in `df`."
        )

    language_symbol_fraction_stack_frame_percentage_bar_chart
    return


@app.cell(hide_code=True)
def _(df):
    _mean_bytes_per_sample_summary = (
        df.select(
            pl.col("mean_bytes_per_sample").median().alias("median"),
            pl.col("mean_bytes_per_sample").mean().alias("mean"),
            pl.col("mean_bytes_per_sample").quantile(0.99).alias("q99"),
            pl.col("mean_bytes_per_sample").quantile(0.999).alias("q99_9"),
            pl.col("mean_bytes_per_sample").max().alias("max"),
        )
        .row(0, named=True)
    )
    _mean_bytes_per_sample_reference_lines = pl.DataFrame(
        {
            "statistic": ["Median", "Mean", "p99", "p99.9", "Max"],
            "value": [
                _mean_bytes_per_sample_summary["median"],
                _mean_bytes_per_sample_summary["mean"],
                _mean_bytes_per_sample_summary["q99"],
                _mean_bytes_per_sample_summary["q99_9"],
                _mean_bytes_per_sample_summary["max"],
            ],
            "label": [
                f"Median: {_mean_bytes_per_sample_summary['median']:,.2f}",
                f"Mean: {_mean_bytes_per_sample_summary['mean']:,.2f}",
                f"p99: {_mean_bytes_per_sample_summary['q99']:,.2f}",
                f"p99.9: {_mean_bytes_per_sample_summary['q99_9']:,.2f}",
                f"Max: {_mean_bytes_per_sample_summary['max']:,.2f}",
            ],
        }
    )

    _mean_bytes_per_sample_histogram_bars = (
        alt.Chart(df)
        .mark_bar(color="#F58518")
        .encode(
            x=alt.X(
                "mean_bytes_per_sample:Q",
                bin=alt.Bin(maxbins=40),
                title="Mean bytes per sample",
            ),
            y=alt.Y("count():Q", title="Count"),
            tooltip=[
                alt.Tooltip("count():Q", title="Count"),
            ],
        )
    )

    _mean_bytes_per_sample_reference_rules = (
        alt.Chart(_mean_bytes_per_sample_reference_lines)
        .mark_rule(strokeWidth=2)
        .encode(
            x=alt.X("value:Q", title="Mean bytes per sample"),
            color=alt.Color(
                "label:N",
                title="Statistic",
                scale=alt.Scale(scheme="category10"),
            ),
            tooltip=[
                alt.Tooltip("statistic:N", title="Statistic"),
                alt.Tooltip("value:Q", title="Value", format=",.2f"),
            ],
        )
    )

    mean_bytes_per_sample_histogram = (
        alt.layer(
            _mean_bytes_per_sample_histogram_bars,
            _mean_bytes_per_sample_reference_rules,
        )
        .properties(
            title="Histogram of mean_bytes_per_sample",
            width=700,
            height=400,
        )
        .interactive()
    )

    mean_bytes_per_sample_histogram
    return


@app.cell(hide_code=True)
def _(df):
    _mean_samples_per_stack_summary = (
        df.select(
            pl.col("mean_samples_per_stack").median().alias("median"),
            pl.col("mean_samples_per_stack").mean().alias("mean"),
            pl.col("mean_samples_per_stack").quantile(0.99).alias("q99"),
            pl.col("mean_samples_per_stack").quantile(0.999).alias("q99_9"),
            pl.col("mean_samples_per_stack").max().alias("max"),
        )
        .row(0, named=True)
    )
    _mean_samples_per_stack_reference_lines = pl.DataFrame(
        {
            "statistic": ["Median", "Mean", "p99", "p99.9", "Max"],
            "value": [
                _mean_samples_per_stack_summary["median"],
                _mean_samples_per_stack_summary["mean"],
                _mean_samples_per_stack_summary["q99"],
                _mean_samples_per_stack_summary["q99_9"],
                _mean_samples_per_stack_summary["max"],
            ],
            "label": [
                f"Median: {_mean_samples_per_stack_summary['median']:,.2f}",
                f"Mean: {_mean_samples_per_stack_summary['mean']:,.2f}",
                f"p99: {_mean_samples_per_stack_summary['q99']:,.2f}",
                f"p99.9: {_mean_samples_per_stack_summary['q99_9']:,.2f}",
                f"Max: {_mean_samples_per_stack_summary['max']:,.2f}",
            ],
        }
    )

    _mean_samples_per_stack_histogram_bars = (
        alt.Chart(df)
        .mark_bar(color="#B279A2")
        .encode(
            x=alt.X(
                "mean_samples_per_stack:Q",
                bin=alt.Bin(maxbins=40),
                title="Mean samples per stack",
            ),
            y=alt.Y("count():Q", title="Count"),
            tooltip=[
                alt.Tooltip("count():Q", title="Count"),
            ],
        )
    )

    _mean_samples_per_stack_reference_rules = (
        alt.Chart(_mean_samples_per_stack_reference_lines)
        .mark_rule(strokeWidth=2)
        .encode(
            x=alt.X("value:Q", title="Mean samples per stack"),
            color=alt.Color(
                "label:N",
                title="Statistic",
                scale=alt.Scale(scheme="category10"),
            ),
            tooltip=[
                alt.Tooltip("statistic:N", title="Statistic"),
                alt.Tooltip("value:Q", title="Value", format=",.2f"),
            ],
        )
    )

    mean_samples_per_stack_histogram = (
        alt.layer(
            _mean_samples_per_stack_histogram_bars,
            _mean_samples_per_stack_reference_rules,
        )
        .properties(
            title="Histogram of mean_samples_per_stack",
            width=700,
            height=400,
        )
        .interactive()
    )

    mean_samples_per_stack_histogram
    return


@app.cell(hide_code=True)
def _(df):
    _mean_attrs_per_resource_summary = (
        df.select(
            pl.col("mean_attrs_per_resource").median().alias("median"),
            pl.col("mean_attrs_per_resource").mean().alias("mean"),
            pl.col("mean_attrs_per_resource").quantile(0.99).alias("q99"),
            pl.col("mean_attrs_per_resource").quantile(0.999).alias("q99_9"),
            pl.col("mean_attrs_per_resource").max().alias("max"),
        )
        .row(0, named=True)
    )
    _mean_attrs_per_resource_reference_lines = pl.DataFrame(
        {
            "statistic": ["Median", "Mean", "p99", "p99.9", "Max"],
            "value": [
                _mean_attrs_per_resource_summary["median"],
                _mean_attrs_per_resource_summary["mean"],
                _mean_attrs_per_resource_summary["q99"],
                _mean_attrs_per_resource_summary["q99_9"],
                _mean_attrs_per_resource_summary["max"],
            ],
            "label": [
                f"Median: {_mean_attrs_per_resource_summary['median']:,.2f}",
                f"Mean: {_mean_attrs_per_resource_summary['mean']:,.2f}",
                f"p99: {_mean_attrs_per_resource_summary['q99']:,.2f}",
                f"p99.9: {_mean_attrs_per_resource_summary['q99_9']:,.2f}",
                f"Max: {_mean_attrs_per_resource_summary['max']:,.2f}",
            ],
        }
    )

    _mean_attrs_per_resource_histogram_bars = (
        alt.Chart(df)
        .mark_bar(color="#72B7B2")
        .encode(
            x=alt.X(
                "mean_attrs_per_resource:Q",
                bin=alt.Bin(maxbins=40),
                title="Mean attrs per resource",
            ),
            y=alt.Y("count():Q", title="Count"),
            tooltip=[
                alt.Tooltip("count():Q", title="Count"),
            ],
        )
    )

    _mean_attrs_per_resource_reference_rules = (
        alt.Chart(_mean_attrs_per_resource_reference_lines)
        .mark_rule(strokeWidth=2)
        .encode(
            x=alt.X("value:Q", title="Mean attrs per resource"),
            color=alt.Color(
                "label:N",
                title="Statistic",
                scale=alt.Scale(scheme="category10"),
            ),
            tooltip=[
                alt.Tooltip("statistic:N", title="Statistic"),
                alt.Tooltip("value:Q", title="Value", format=",.2f"),
            ],
        )
    )

    mean_attrs_per_resource_histogram = (
        alt.layer(
            _mean_attrs_per_resource_histogram_bars,
            _mean_attrs_per_resource_reference_rules,
        )
        .properties(
            title="Histogram of mean_attrs_per_resource",
            width=700,
            height=400,
        )
        .interactive()
    )

    mean_attrs_per_resource_histogram
    return


@app.cell(hide_code=True)
def _(df):
    bytes_point_cloud = (
        alt.Chart(df)
        .mark_circle(size=45, opacity=0.55, color="#4C78A8")
        .encode(
            x=alt.X(
                "mean_bytes_per_sample:Q",
                title="Mean bytes per sample",
                scale=alt.Scale(zero=False),
            ),
            y=alt.Y(
                "uncompressed_mib:Q",
                title="Uncompressed profile size (MiB)",
                scale=alt.Scale(zero=False),
            ),
            tooltip=[
                alt.Tooltip("mean_bytes_per_sample:Q", title="Mean bytes per sample", format=",.2f"),
                alt.Tooltip("uncompressed_mib:Q", title="Uncompressed size (MiB)", format=",.2f"),
                alt.Tooltip("samples_count:Q", title="Samples count", format=",.0f"),
                alt.Tooltip("profiles_count:Q", title="Profiles count", format=",.0f"),
            ],
        )
        .properties(
            title="Uncompressed profile size vs. mean bytes per sample",
            width=700,
            height=450,
        )
        .interactive()
    )

    bytes_point_cloud
    return


@app.cell(hide_code=True)
def _(df):
    dict_strings_point_cloud = (
        alt.Chart(df)
        .mark_circle(size=45, opacity=0.55, color="#54A24B")
        .encode(
            x=alt.X(
                "mean_bytes_per_sample:Q",
                title="Mean bytes per sample",
                scale=alt.Scale(zero=False),
            ),
            y=alt.Y(
                "dict_strings_count:Q",
                title="Dictionary strings count",
                scale=alt.Scale(zero=False),
            ),
            tooltip=[
                alt.Tooltip("mean_bytes_per_sample:Q", title="Mean bytes per sample", format=",.2f"),
                alt.Tooltip("dict_strings_count:Q", title="Dictionary strings count", format=",.0f"),
                alt.Tooltip("uncompressed_mib:Q", title="Uncompressed size (MiB)", format=",.2f"),
                alt.Tooltip("samples_count:Q", title="Samples count", format=",.0f"),
            ],
        )
        .properties(
            title="Dictionary strings count vs. mean bytes per sample",
            width=700,
            height=450,
        )
        .interactive()
    )

    dict_strings_point_cloud
    return


@app.cell(hide_code=True)
def _(df):
    samples_count_point_cloud = (
        alt.Chart(df)
        .mark_circle(size=45, opacity=0.55, color="#B279A2")
        .encode(
            x=alt.X(
                "mean_bytes_per_sample:Q",
                title="Mean bytes per sample",
                scale=alt.Scale(zero=False),
            ),
            y=alt.Y(
                "samples_count:Q",
                title="Samples count",
                scale=alt.Scale(zero=False),
            ),
            tooltip=[
                alt.Tooltip("mean_bytes_per_sample:Q", title="Mean bytes per sample", format=",.2f"),
                alt.Tooltip("samples_count:Q", title="Samples count", format=",.0f"),
                alt.Tooltip("uncompressed_mib:Q", title="Uncompressed size (MiB)", format=",.2f"),
                alt.Tooltip("profiles_count:Q", title="Profiles count", format=",.0f"),
            ],
        )
        .properties(
            title="Samples count vs. mean bytes per sample",
            width=700,
            height=450,
        )
        .interactive()
    )

    samples_count_point_cloud
    return


@app.cell(hide_code=True)
def _(df):
    mean_cpu_usage_point_cloud = (
        alt.Chart(df)
        .mark_circle(size=45, opacity=0.55, color="#E45756")
        .encode(
            x=alt.X(
                "mean_bytes_per_sample:Q",
                title="Mean bytes per sample",
                scale=alt.Scale(zero=False),
            ),
            y=alt.Y(
                "mean_cpu_usage_cores:Q",
                title="Mean CPU usage (cores)",
                scale=alt.Scale(zero=False),
            ),
            tooltip=[
                alt.Tooltip("mean_bytes_per_sample:Q", title="Mean bytes per sample", format=",.2f"),
                alt.Tooltip("mean_cpu_usage_cores:Q", title="Mean CPU usage (cores)", format=",.2f"),
                alt.Tooltip("samples_count:Q", title="Samples count", format=",.0f"),
                alt.Tooltip("duration_sec:Q", title="Duration seconds", format=",.2f"),
                alt.Tooltip("uncompressed_mib:Q", title="Uncompressed size (MiB)", format=",.2f"),
            ],
        )
        .properties(
            title="Mean CPU usage vs. mean bytes per sample",
            width=700,
            height=450,
        )
        .interactive()
    )

    mean_cpu_usage_point_cloud
    return


@app.cell(hide_code=True)
def _(df):
    _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_data = df.select(
        "uncompressed_mib",
        "mean_cpu_usage_cores",
        "mean_bytes_per_sample",
        "mean_attrs_per_resource",
        "samples_count",
        "duration_sec",
        "mean_samples_per_stack",
    )

    _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_stats = (
        _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_data.select(
            pl.col("uncompressed_mib").mean().alias("x_mean"),
            pl.col("mean_cpu_usage_cores").mean().alias("y_mean"),
            pl.col("uncompressed_mib").min().alias("x_min"),
            pl.col("uncompressed_mib").max().alias("x_max"),
        ).row(0, named=True)
    )

    _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_slope = (
        _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_data.select(
            (
                (
                    (
                        pl.col("uncompressed_mib")
                        - _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_stats["x_mean"]
                    )
                    * (
                        pl.col("mean_cpu_usage_cores")
                        - _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_stats["y_mean"]
                    )
                ).sum()
                / (
                    (
                        pl.col("uncompressed_mib")
                        - _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_stats["x_mean"]
                    )
                    ** 2
                ).sum()
            ).alias("slope")
        ).item()
    )

    _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_intercept = (
        _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_stats["y_mean"]
        - _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_slope
        * _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_stats["x_mean"]
    )

    _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_regression_data = pl.DataFrame(
        {
            "uncompressed_mib": [
                _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_stats["x_min"],
                _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_stats["x_max"],
            ],
        }
    ).with_columns(
        (
            _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_intercept
            + _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_slope
            * pl.col("uncompressed_mib")
        ).alias("mean_cpu_usage_cores"),
        pl.lit(
            f"Slope: {1 / _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_slope:.2f} MiB per active CPU core"
        ).alias("regression_label"),
    )

    _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_base = alt.Chart(
        _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_data
    ).encode(
        x=alt.X(
            "uncompressed_mib:Q",
            title="Uncompressed profile size (MiB)",
            scale=alt.Scale(zero=False),
        ),
        y=alt.Y(
            "mean_cpu_usage_cores:Q",
            title="Mean CPU usage (cores)",
            scale=alt.Scale(zero=False),
        ),
    )

    _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_points = (
        _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_base.mark_circle(
            size=45,
            opacity=0.55,
        ).encode(
            color=alt.Color(
                "mean_samples_per_stack:Q",
                title="Mean samples per stack",
                scale=alt.Scale(scheme="viridis"),
            ),
            tooltip=[
                alt.Tooltip("uncompressed_mib:Q", title="Uncompressed size (MiB)", format=",.2f"),
                alt.Tooltip("mean_cpu_usage_cores:Q", title="Mean CPU usage (cores)", format=",.2f"),
                alt.Tooltip("mean_samples_per_stack:Q", title="Mean samples per stack", format=",.2f"),
                alt.Tooltip("mean_bytes_per_sample:Q", title="Mean bytes per sample", format=",.2f"),
                alt.Tooltip("mean_attrs_per_resource:Q", title="Mean attrs per resource", format=",.2f"),
                alt.Tooltip("samples_count:Q", title="Samples count", format=",.0f"),
                alt.Tooltip("duration_sec:Q", title="Duration seconds", format=",.2f"),
            ],
        )
    )

    _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_regression_line = (
        alt.Chart(_mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_regression_data)
        .mark_line(strokeWidth=3)
        .encode(
            x=alt.X("uncompressed_mib:Q", title="Uncompressed profile size (MiB)"),
            y=alt.Y("mean_cpu_usage_cores:Q", title="Mean CPU usage (cores)"),
            color=alt.Color(
                "regression_label:N",
                title="Linear regression",
                scale=alt.Scale(range=["#2F4B7C"]),
            ),
            tooltip=[
                alt.Tooltip("uncompressed_mib:Q", title="Uncompressed size (MiB)", format=",.2f"),
                alt.Tooltip("mean_cpu_usage_cores:Q", title="Fitted mean CPU usage (cores)", format=",.2f"),
                alt.Tooltip("regression_label:N", title="Regression slope"),
            ],
        )
    )

    mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_point_cloud = (
        alt.layer(
            _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_points,
            _mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_regression_line,
        )
        .properties(
            title="Mean CPU usage vs. uncompressed profile size colored by mean samples per stack",
            width=700,
            height=450,
        )
        .interactive()
    )

    mean_cpu_usage_vs_uncompressed_size_by_mean_samples_per_stack_point_cloud
    return


@app.cell(hide_code=True)
def _(df):
    mean_attrs_per_resource_vs_mean_bytes_per_sample_point_cloud = (
        alt.Chart(df)
        .mark_circle(size=45, opacity=0.55, color="#72B7B2")
        .encode(
            x=alt.X(
                "mean_bytes_per_sample:Q",
                title="Mean bytes per sample",
                scale=alt.Scale(zero=False),
            ),
            y=alt.Y(
                "mean_attrs_per_resource:Q",
                title="Mean attributes per resource",
                scale=alt.Scale(zero=False),
            ),
            tooltip=[
                alt.Tooltip("mean_bytes_per_sample:Q", title="Mean bytes per sample", format=",.2f"),
                alt.Tooltip("mean_attrs_per_resource:Q", title="Mean attributes per resource", format=",.2f"),
                alt.Tooltip("resource_profiles_attrs_count:Q", title="Resource profile attributes count", format=",.0f"),
                alt.Tooltip("resource_profiles_count:Q", title="Resource profiles count", format=",.0f"),
                alt.Tooltip("uncompressed_mib:Q", title="Uncompressed size (MiB)", format=",.2f"),
                alt.Tooltip("samples_count:Q", title="Samples count", format=",.0f"),
            ],
        )
        .properties(
            title="Mean attributes per resource vs. mean bytes per sample",
            width=700,
            height=450,
        )
        .interactive()
    )

    mean_attrs_per_resource_vs_mean_bytes_per_sample_point_cloud
    return


@app.cell(hide_code=True)
def _(df):
    mean_bytes_per_sample_correlations = (
        pl.DataFrame(
            {
                "column": [
                    column_name
                    for column_name, column_dtype in df.schema.items()
                    if column_dtype.is_numeric()
                    and column_name not in ["mean_bytes_per_sample", "uncompressed_bytes"]
                ],
                "pearson_correlation": [
                    df.select(
                        pl.corr("mean_bytes_per_sample", column_name).alias(
                            "correlation"
                        )
                    ).item()
                    for column_name, column_dtype in df.schema.items()
                    if column_dtype.is_numeric()
                    and column_name not in ["mean_bytes_per_sample", "uncompressed_bytes"]
                ],
            }
        )
        .with_columns(
            pl.col("pearson_correlation")
            .abs()
            .alias("absolute_correlation")
        )
        .filter(pl.col("pearson_correlation").is_finite())
        .sort("absolute_correlation", descending=True)
    )

    mean_bytes_per_sample_correlation_chart = (
        alt.Chart(mean_bytes_per_sample_correlations)
        .mark_bar()
        .encode(
            x=alt.X(
                "pearson_correlation:Q",
                title="Pearson correlation with mean_bytes_per_sample",
                scale=alt.Scale(domain=[-0.5, 0.5]),
            ),
            y=alt.Y(
                "column:N",
                sort="-x",
                title="Column",
            ),
            color=alt.condition(
                alt.datum.pearson_correlation >= 0,
                alt.value("#54A24B"),
                alt.value("#E45756"),
            ),
            tooltip=[
                alt.Tooltip("column:N", title="Column"),
                alt.Tooltip(
                    "pearson_correlation:Q",
                    title="Pearson correlation",
                    format=".3f",
                ),
                alt.Tooltip(
                    "absolute_correlation:Q",
                    title="Absolute correlation",
                    format=".3f",
                ),
            ],
        )
        .properties(
            title="Correlation with mean_bytes_per_sample",
            width=700,
            height=800,
        )
        .interactive()
    )

    mean_bytes_per_sample_correlation_chart
    return


if __name__ == "__main__":
    app.run()

# Predefined query patterns with placeholders for parameters
RESEARCH_QUERY_PATTERNS = {
    "time_series": """
        from(bucket: "{bucket}")
        |> range(start: {start}T00:00:00Z, stop: {stop}T23:59:59Z)
        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
        |> filter(fn: (r) => r["ticker"] =~ /({tickers})/)
        |> filter(fn: (r) => r["_field"] =~ /({fields})/)
        |> aggregateWindow(every: {every}, fn: {fn})
        |> pivot(rowKey: ["_time", "ticker"], columnKey: ["_field"], valueColumn: "_value")
    """,
    "aggregate_summary": """
        from(bucket: "{bucket}")
        |> range(start: {start}T00:00:00Z, stop: {stop}T23:59:59Z)
        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
        |> filter(fn: (r) => r["ticker"] =~ /({tickers})/)
        |> filter(fn: (r) => r["_field"] =~ /({fields})/)
        |> group(columns: ["ticker"])
        |> aggregateWindow(every: {every}, fn: {fn})
        |> yield(name: "summary")
    """,
    "correlation": """
        from(bucket: "{bucket}")
        |> range(start: {start}T00:00:00Z, stop: {stop}T23:59:59Z)
        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
        |> filter(fn: (r) => r["ticker"] =~ /({tickers})/)
        |> filter(fn: (r) => r["_field"] =~ /({fields})/)
        |> pivot(rowKey: ["_time", "ticker"], columnKey: ["_field"], valueColumn: "_value")
        |> map(fn: (r) => ({{ r with correlation: // Add correlation logic }}))
    """,
    "filtered_by_tag": """
        from(bucket: "{bucket}")
        |> range(start: {start}T00:00:00Z, stop: {stop}T23:59:59Z)
        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
        |> filter(fn: (r) => r["{tag_key}"] =~ /({tag_value})/)
        |> filter(fn: (r) => r["_field"] =~ /({fields})/)
        |> aggregateWindow(every: {every}, fn: {fn})
        |> pivot(rowKey: ["_time", "ticker"], columnKey: ["_field"], valueColumn: "_value")
    """,
    "sampled_data": """
        from(bucket: "{bucket}")
        |> range(start: {start}T00:00:00Z, stop: {stop}T23:59:59Z)
        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
        |> filter(fn: (r) => r["ticker"] =~ /({tickers})/)
        |> filter(fn: (r) => r["_field"] =~ /({fields})/)
        |> sample(n: {sample_size})
        |> pivot(rowKey: ["_time", "ticker"], columnKey: ["_field"], valueColumn: "_value")
    """
}
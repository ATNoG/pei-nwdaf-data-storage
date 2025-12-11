class QueryIF:
    between = """
    from(bucket: "{bucket}")
      |> range(start: {start_time}, stop: {end_time})
      |> filter(fn: (r) => r["cell_index"] == "{cell_index}")
      |> limit(n: {limit}, offset: {offset})
    """

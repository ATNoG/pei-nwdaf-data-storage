class QueryIF:
    query_by_tags = """
from(bucket: "{bucket}")
  |> range(start: {start_time}, stop: {end_time})
  |> filter(fn: (r) => r._measurement == "{measurement}"{tag_filters})
  |> limit(n: {limit}, offset: {offset})
"""

    get_fields = """
import "influxdata/influxdb/schema"

schema.fieldKeys(
    bucket: "{bucket}",
    predicate: (r) => r._measurement == "{measurement}"
)
"""

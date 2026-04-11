from pipelines.bronze_to_silver.example import (
    example_pipeline
)

from pipelines.silver_to_gold.example import (
    example_to_gold_events,
    example_to_gold_daily
)

pipeline_registry = {
    "example": example_pipeline,
    "example_to_gold_events": example_to_gold_events,
    "example_to_gold_daily": example_to_gold_daily,
}
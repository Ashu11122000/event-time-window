
import apache_beam as beam

from apache_beam.transforms.window import FixedWindows

from src.events import get_sample_events

from src.transforms import (
    assign_event_timestamp,
    convert_to_order_metrics,
    sum_order_metrics,
    format_window_result,
)


def run_pipeline():

    events = get_sample_events()

    with beam.Pipeline() as pipeline:

        (
            pipeline
            | "Read Events" >> beam.Create(events)

            | "Assign Event Timestamps"
            >> beam.Map(assign_event_timestamp)

            | "Create Five Minute Windows"
            >> beam.WindowInto(FixedWindows(300))

            | "Convert To Metrics"
            >> beam.Map(convert_to_order_metrics)

            | "Aggregate Order Metrics"
            >> beam.CombinePerKey(sum_order_metrics)

            | "Format Window Results"
            >> beam.Map(format_window_result)

            | "Display Results"
            >> beam.Map(print)
        )


if __name__ == "__main__":
    run_pipeline()

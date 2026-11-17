
import apache_beam as beam

from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import (
    AfterWatermark,
    AfterCount,
    AccumulationMode,
)
from apache_beam.utils.timestamp import Timestamp
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
)


# =====================================================
# CONFIGURATION
# =====================================================

WINDOW_SIZE_SECONDS = 300
ALLOWED_LATENESS_SECONDS = 120


# =====================================================
# 1. CREATE SIMULATED STREAMING EVENT SOURCE
# =====================================================

def create_late_event_stream():
    """
    Generate streaming events with:
    - Three initial events
    - One out-of-order late event
    - Explicit watermark advancement
    """

    stream = TestStream()

    # Initial events for the 10:00-10:05 window.
    stream = stream.add_elements([

        beam.window.TimestampedValue(
            {"event_id": "E1", "amount": 500},
            Timestamp.from_rfc3339(
                "2026-11-17T10:01:00Z"
            )
        ),

        beam.window.TimestampedValue(
            {"event_id": "E2", "amount": 700},
            Timestamp.from_rfc3339(
                "2026-11-17T10:02:00Z"
            )
        ),

        beam.window.TimestampedValue(
            {"event_id": "E3", "amount": 300},
            Timestamp.from_rfc3339(
                "2026-11-17T10:04:00Z"
            )
        ),

    ])

    # Advance watermark beyond the first window's end.
    # Window ends at 10:05.
    # Watermark advances to 10:06.

    stream = stream.advance_watermark_to(
        Timestamp.from_rfc3339(
            "2026-11-17T10:06:00Z"
        )
    )

    # E4 arrives after the watermark.
    # Its event timestamp is 10:03.
    # It should be accepted within allowed lateness.

    stream = stream.add_elements([

        beam.window.TimestampedValue(
            {"event_id": "E4", "amount": 200},
            Timestamp.from_rfc3339(
                "2026-11-17T10:03:00Z"
            )
        ),

    ])

    return stream


# =====================================================
# 2. FORMAT WINDOW RESULTS
# =====================================================

class DisplayWindowResult(beam.DoFn):
    """
    Display window aggregation and pane information.
    """

    def process(
        self,
        element,
        window=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam
    ):

        key, events = element

        events = list(events)

        total_amount = sum(
            event["amount"]
            for event in events
        )

        total_orders = len(events)

        timing_names = {
            0: "EARLY",
            1: "ON_TIME",
            2: "LATE"
        }

        result = {
            "window_start": (
                window.start.to_utc_datetime().isoformat()
            ),

            "window_end": (
                window.end.to_utc_datetime().isoformat()
            ),

            "total_amount": total_amount,

            "total_orders": total_orders,

            "event_ids": [
                event["event_id"]
                for event in events
            ],

            "pane_timing": timing_names.get(
                int(pane_info.timing),
                "UNKNOWN"
            ),

            "is_first": pane_info.is_first,

            "is_last": pane_info.is_last,
        }

        yield result


# =====================================================
# 3. BUILD AND EXECUTE STREAMING PIPELINE
# =====================================================

def run_late_event_pipeline():
    """
    Execute Apache Beam streaming pipeline with:

    - Five-minute tumbling windows
    - Watermark-based initial firing
    - Two-minute allowed lateness
    - Late event trigger
    - Accumulating results
    """

    # Explicitly enable streaming mode.
    options = PipelineOptions()

    options.view_as(
        StandardOptions
    ).streaming = True

    with beam.Pipeline(options=options) as pipeline:

        # Read simulated streaming events.
        events = (
            pipeline

            | "Read Late Event Stream"
            >> create_late_event_stream()
        )

        # Apply event-time windows and triggers.
        windowed_events = (
            events

            | "Apply Windows And Allowed Lateness"
            >> beam.WindowInto(

                FixedWindows(
                    WINDOW_SIZE_SECONDS
                ),

                trigger=AfterWatermark(

                    late=AfterCount(1)

                ),

                accumulation_mode=(
                    AccumulationMode.ACCUMULATING
                ),

                allowed_lateness=(
                    ALLOWED_LATENESS_SECONDS
                )
            )
        )

        # Assign aggregation key.
        keyed_events = (
            windowed_events

            | "Assign Aggregation Key"
            >> beam.Map(
                lambda event: (
                    "orders",
                    event
                )
            )
        )

        # Group events by key and window.
        grouped_events = (
            keyed_events

            | "Group Events By Window"
            >> beam.GroupByKey()
        )

        # Format each window firing.
        results = (
            grouped_events

            | "Format Window Results"
            >> beam.ParDo(
                DisplayWindowResult()
            )
        )

        # Display results.
        results | "Print Window Results" >> beam.Map(print)


# =====================================================
# 4. MAIN EXECUTION
# =====================================================

if __name__ == "__main__":
    run_late_event_pipeline()

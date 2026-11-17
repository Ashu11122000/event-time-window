
import apache_beam as beam

from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import FixedWindows
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

TOO_LATE_TAG = "too_late"


# =====================================================
# 1. CREATE SIMULATED STREAMING EVENT SOURCE
# =====================================================

def create_event_stream():
    """
    Generate events with different arrival watermarks.

    E1, E2, E3: On-time events.
    E4: Late but within allowed lateness.
    E6: Arrives after allowed lateness expires.
    """

    stream = TestStream()

    # -------------------------------------------------
    # Initial events
    # Arrival watermark: 09:59
    # -------------------------------------------------

    stream = stream.add_elements([

        beam.window.TimestampedValue(
            {
                "event_id": "E1",
                "amount": 500,
                "arrival_watermark": "2026-11-17T09:59:00Z",
            },
            Timestamp.from_rfc3339(
                "2026-11-17T10:01:00Z"
            )
        ),

        beam.window.TimestampedValue(
            {
                "event_id": "E2",
                "amount": 700,
                "arrival_watermark": "2026-11-17T09:59:00Z",
            },
            Timestamp.from_rfc3339(
                "2026-11-17T10:02:00Z"
            )
        ),

        beam.window.TimestampedValue(
            {
                "event_id": "E3",
                "amount": 300,
                "arrival_watermark": "2026-11-17T09:59:00Z",
            },
            Timestamp.from_rfc3339(
                "2026-11-17T10:04:00Z"
            )
        ),

    ])

    # -------------------------------------------------
    # Advance watermark beyond window end.
    # Window end: 10:05
    # Watermark: 10:06
    # -------------------------------------------------

    stream = stream.advance_watermark_to(
        Timestamp.from_rfc3339(
            "2026-11-17T10:06:00Z"
        )
    )

    # -------------------------------------------------
    # E4: Late but acceptable.
    # Event time: 10:03
    # Arrival watermark: 10:06
    # Cleanup deadline: 10:07
    # -------------------------------------------------

    stream = stream.add_elements([

        beam.window.TimestampedValue(
            {
                "event_id": "E4",
                "amount": 200,
                "arrival_watermark": "2026-11-17T10:06:00Z",
            },
            Timestamp.from_rfc3339(
                "2026-11-17T10:03:00Z"
            )
        ),

    ])

    # -------------------------------------------------
    # Advance watermark beyond cleanup deadline.
    # Cleanup deadline: 10:07
    # -------------------------------------------------

    stream = stream.advance_watermark_to(
        Timestamp.from_rfc3339(
            "2026-11-17T10:07:01Z"
        )
    )

    # -------------------------------------------------
    # E6: Arrives after allowed lateness.
    # Event time: 10:04
    # Arrival watermark: 10:07:01
    # -------------------------------------------------

    stream = stream.add_elements([

        beam.window.TimestampedValue(
            {
                "event_id": "E6",
                "amount": 1000,
                "arrival_watermark": "2026-11-17T10:07:01Z",
            },
            Timestamp.from_rfc3339(
                "2026-11-17T10:04:00Z"
            )
        ),

    ])

    return stream


# =====================================================
# 2. CLASSIFY EVENTS
# =====================================================

class ClassifyLateEvents(beam.DoFn):
    """
    Classify events as accepted or too late.

    The arrival watermark is explicitly included in
    each simulated event for deterministic testing.

    Beam's TimestampParam provides the actual event
    timestamp assigned by TestStream.
    """

    def process(
        self,
        event,
        timestamp=beam.DoFn.TimestampParam
    ):

        event_seconds = timestamp.micros / 1_000_000

        arrival_watermark = Timestamp.from_rfc3339(
            event["arrival_watermark"]
        )

        arrival_watermark_seconds = (
            arrival_watermark.micros / 1_000_000
        )

        # Calculate the fixed window boundaries.
        window_start = (
            int(event_seconds // WINDOW_SIZE_SECONDS)
            * WINDOW_SIZE_SECONDS
        )

        window_end = (
            window_start + WINDOW_SIZE_SECONDS
        )

        cleanup_deadline = (
            window_end + ALLOWED_LATENESS_SECONDS
        )

        # An event is too late when the arrival
        # watermark has passed its cleanup deadline.
        if arrival_watermark_seconds > cleanup_deadline:

            yield beam.pvalue.TaggedOutput(
                TOO_LATE_TAG,
                {
                    "event_id": event["event_id"],
                    "amount": event["amount"],
                    "event_time": timestamp.to_rfc3339(),
                    "arrival_watermark": (
                        event["arrival_watermark"]
                    ),
                    "cleanup_deadline": cleanup_deadline,
                    "status": "TOO_LATE"
                }
            )

        else:

            event["status"] = (
                "LATE_ACCEPTED"
                if arrival_watermark_seconds > window_end
                else "ON_TIME"
            )

            yield event


# =====================================================
# 3. FORMAT ACCEPTED WINDOW RESULTS
# =====================================================

def format_accepted_result(element):

    order_key, events = element

    total_amount = sum(
        event["amount"] for event in events
    )

    total_orders = len(events)

    event_ids = [
        event["event_id"] for event in events
    ]

    statuses = [
        event["status"] for event in events
    ]

    return {
        "order_key": order_key,
        "total_amount": total_amount,
        "total_orders": total_orders,
        "event_ids": event_ids,
        "statuses": statuses
    }


# =====================================================
# 4. BUILD STREAMING PIPELINE
# =====================================================

def run_late_routing_pipeline():

    options = PipelineOptions()

    options.view_as(
        StandardOptions
    ).streaming = True

    with beam.Pipeline(options=options) as pipeline:

        events = (
            pipeline
            | "Read Event Stream"
            >> create_event_stream()
        )

        # Classify events and create a side output.
        classified = (
            events
            | "Classify Late Events"
            >> beam.ParDo(
                ClassifyLateEvents()
            ).with_outputs(
                TOO_LATE_TAG,
                main="accepted"
            )
        )

        accepted_events = classified.accepted

        too_late_events = classified[TOO_LATE_TAG]

        # Apply event-time windows to accepted events.
        # Preserve events arriving within the allowed
        # lateness period.
        windowed_accepted = (
            accepted_events
            | "Apply Five Minute Windows"
            >> beam.WindowInto(
                FixedWindows(WINDOW_SIZE_SECONDS),
                allowed_lateness=ALLOWED_LATENESS_SECONDS
            )
        )

        # Group accepted events by order key.
        grouped_accepted = (
            windowed_accepted
            | "Create Accepted Order Keys"
            >> beam.Map(
                lambda event: ("orders", event)
            )
            | "Group Accepted Orders"
            >> beam.GroupByKey()
        )

        # Calculate total amount and number of orders.
        accepted_results = (
            grouped_accepted
            | "Format Accepted Results"
            >> beam.Map(format_accepted_result)
        )

        # Display accepted results.
        accepted_results | "Display Accepted Results" >> beam.Map(
            lambda result: print(
                "ACCEPTED WINDOW RESULT:",
                result
            )
        )

        # Display too-late events separately.
        too_late_events | "Display Too Late Events" >> beam.Map(
            lambda event: print(
                "TOO-LATE SIDE OUTPUT:",
                event
            )
        )


# =====================================================
# 5. MAIN EXECUTION
# =====================================================

if __name__ == "__main__":
    run_late_routing_pipeline()

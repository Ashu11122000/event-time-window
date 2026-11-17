
import apache_beam as beam

from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Timestamp


WATERMARK_DELAY_SECONDS = 5 * 60


def calculate_watermark(max_event_timestamp):
    """
    Simulate a bounded-out-of-orderness watermark.

    Watermark = Maximum observed event time - 5 minutes.
    """
    return max_event_timestamp - WATERMARK_DELAY_SECONDS


def create_watermark_test_stream():

    test_stream = TestStream()

    # Initial events
    test_stream = (
        test_stream
        .add_elements([
            beam.window.TimestampedValue(
                {"event_id": "E1", "amount": 500},
                Timestamp.from_rfc3339("2026-11-17T10:01:00Z")
            ),
            beam.window.TimestampedValue(
                {"event_id": "E2", "amount": 700},
                Timestamp.from_rfc3339("2026-11-17T10:02:00Z")
            ),
            beam.window.TimestampedValue(
                {"event_id": "E3", "amount": 300},
                Timestamp.from_rfc3339("2026-11-17T10:04:00Z")
            ),
        ])
    )

    # Maximum observed event time = 10:04.
    # Watermark = 09:59.
    test_stream = test_stream.advance_watermark_to(
        Timestamp.from_rfc3339("2026-11-17T09:59:00Z")
    )

    # Out-of-order event E4.
    test_stream = test_stream.add_elements([
        beam.window.TimestampedValue(
            {"event_id": "E4", "amount": 200},
            Timestamp.from_rfc3339("2026-11-17T10:03:00Z")
        )
    ])

    # Newer event establishes maximum event time = 10:11.
    # Watermark = 10:06.
    test_stream = test_stream.add_elements([
        beam.window.TimestampedValue(
            {"event_id": "E5", "amount": 900},
            Timestamp.from_rfc3339("2026-11-17T10:11:00Z")
        )
    ])

    test_stream = test_stream.advance_watermark_to(
        Timestamp.from_rfc3339("2026-11-17T10:06:00Z")
    )

    return test_stream


def run_watermark_pipeline():

    with beam.Pipeline() as pipeline:

        events = (
            pipeline
            | "Read Test Stream"
            >> create_watermark_test_stream()
        )

        windowed_events = (
            events
            | "Create Five Minute Windows"
            >> beam.WindowInto(
                FixedWindows(300)
            )
        )

        results = (
            windowed_events
            | "Convert To Key Value"
            >> beam.Map(
                lambda event: (
                    "orders",
                    (event["amount"], 1)
                )
            )
            | "Aggregate Window Results"
            >> beam.CombinePerKey(
                lambda values: (
                    sum(amount for amount, count in values),
                    sum(count for amount, count in values)
                )
            )
        )

        results | "Display Watermark Results" >> beam.Map(print)


def test_watermark_calculation():

    max_timestamp = Timestamp.from_rfc3339(
        "2026-11-17T10:11:00Z"
    ).micros // 1_000_000

    watermark = calculate_watermark(max_timestamp)

    expected = Timestamp.from_rfc3339(
        "2026-11-17T10:06:00Z"
    ).micros // 1_000_000

    assert watermark == expected

    print("Watermark calculation test passed")


if __name__ == "__main__":
    run_watermark_pipeline()


import unittest

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp

from src.late_routing import (
    ClassifyLateEvents,
    TOO_LATE_TAG,
)


class LateEventRoutingTests(unittest.TestCase):

    def setUp(self):
        self.classifier = ClassifyLateEvents()

    # -------------------------------------------------
    # TEST 1: ON-TIME EVENT
    # -------------------------------------------------

    def test_on_time_event_is_accepted(self):

        event = {
            "event_id": "E1",
            "amount": 500,
            "arrival_watermark": "2026-11-17T09:59:00Z",
        }

        timestamp = Timestamp.from_rfc3339(
            "2026-11-17T10:01:00Z"
        )

        results = list(
            self.classifier.process(
                event,
                timestamp=timestamp
            )
        )

        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], dict)
        self.assertEqual(results[0]["status"], "ON_TIME")
        self.assertEqual(results[0]["event_id"], "E1")

    # -------------------------------------------------
    # TEST 2: LATE BUT ACCEPTABLE EVENT
    # -------------------------------------------------

    def test_late_event_within_allowed_lateness(self):

        event = {
            "event_id": "E4",
            "amount": 200,
            "arrival_watermark": "2026-11-17T10:06:00Z",
        }

        timestamp = Timestamp.from_rfc3339(
            "2026-11-17T10:03:00Z"
        )

        results = list(
            self.classifier.process(
                event,
                timestamp=timestamp
            )
        )

        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], dict)
        self.assertEqual(
            results[0]["status"],
            "LATE_ACCEPTED"
        )
        self.assertEqual(results[0]["event_id"], "E4")

    # -------------------------------------------------
    # TEST 3: TOO-LATE EVENT
    # -------------------------------------------------

    def test_too_late_event_goes_to_side_output(self):

        event = {
            "event_id": "E6",
            "amount": 1000,
            "arrival_watermark": "2026-11-17T10:07:01Z",
        }

        timestamp = Timestamp.from_rfc3339(
            "2026-11-17T10:04:00Z"
        )

        results = list(
            self.classifier.process(
                event,
                timestamp=timestamp
            )
        )

        self.assertEqual(len(results), 1)

        side_output = results[0]

        self.assertIsInstance(
            side_output,
            beam.pvalue.TaggedOutput
        )

        self.assertEqual(
            side_output.tag,
            TOO_LATE_TAG
        )

        self.assertEqual(
            side_output.value["event_id"],
            "E6"
        )

        self.assertEqual(
            side_output.value["status"],
            "TOO_LATE"
        )

    # -------------------------------------------------
    # TEST 4: EVENT EXACTLY AT CLEANUP DEADLINE
    # -------------------------------------------------

    def test_event_at_cleanup_deadline_is_not_too_late(self):

        event = {
            "event_id": "E7",
            "amount": 400,
            "arrival_watermark": "2026-11-17T10:07:00Z",
        }

        timestamp = Timestamp.from_rfc3339(
            "2026-11-17T10:04:00Z"
        )

        results = list(
            self.classifier.process(
                event,
                timestamp=timestamp
            )
        )

        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], dict)

        self.assertEqual(
            results[0]["status"],
            "LATE_ACCEPTED"
        )


if __name__ == "__main__":
    unittest.main()

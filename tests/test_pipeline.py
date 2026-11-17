
import unittest

from src.events import get_sample_events, parse_event_time


class EventSourceTests(unittest.TestCase):

    def test_event_generation(self):
        events = get_sample_events()

        self.assertEqual(len(events), 5)
        self.assertEqual(events[0]["event_id"], "E1")

    def test_out_of_order_events(self):
        events = get_sample_events()

        # E4 arrives after E3 but has an earlier event timestamp.
        self.assertEqual(events[3]["event_id"], "E4")

        e3_time = parse_event_time(events[2]["event_time"])
        e4_time = parse_event_time(events[3]["event_time"])

        self.assertLess(e4_time, e3_time)

    def test_timestamp_conversion(self):
        events = get_sample_events()

        timestamp = parse_event_time(events[0]["event_time"])

        self.assertEqual(timestamp.hour, 10)
        self.assertEqual(timestamp.minute, 1)
        self.assertIsNotNone(timestamp.tzinfo)


if __name__ == "__main__":
    unittest.main()

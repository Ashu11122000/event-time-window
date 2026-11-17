
import apache_beam as beam

from src.events import parse_event_time


def assign_event_timestamp(event):
    """
    Assign the actual event timestamp to each event.
    """
    timestamp = parse_event_time(
        event["event_time"]
    ).timestamp()

    return beam.window.TimestampedValue(
        event,
        timestamp
    )


def convert_to_order_metrics(event):
    """
    Convert an event into a key-value pair
    for aggregation.
    """
    return (
        "all_orders",
        (event["amount"], 1)
    )


def sum_order_metrics(values):
    """
    Calculate total order amount and order count.
    """
    total_amount = 0
    total_orders = 0

    for amount, count in values:
        total_amount += amount
        total_orders += count

    return total_amount, total_orders


def format_window_result(element, window=beam.DoFn.WindowParam):
    """
    Format the aggregated window result.
    """
    key, metrics = element
    total_amount, total_orders = metrics

    return {
        "window_start": window.start.to_utc_datetime().isoformat(),
        "window_end": window.end.to_utc_datetime().isoformat(),
        "total_amount": total_amount,
        "total_orders": total_orders,
    }

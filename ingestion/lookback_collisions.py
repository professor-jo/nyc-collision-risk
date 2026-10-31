# Picks the date to start the next data pull from.
#
# We go back a few days (default 7) from the last crash we saved, so we
# also grab crashes that were reported late or changed since last time.

from datetime import datetime, timedelta


def calculate_lookback_start(last_crash_date, days=7):
    '''
    Calculate the start of a historical lookback window.

    Example:
        Last processed crash date = October 8th
        Lookback period = 7 days
        Lookback start = October 1st
    '''

    # Convert the timestamp string into a datetime object
    watermark_date = datetime.fromisoformat(last_crash_date)

    # Subtract the lookback period from the watermark date
    lookback_start = watermark_date - timedelta(days=days)

    # Convert the datetime back into an API-compatible string.
    return lookback_start.isoformat(timespec="milliseconds")

if __name__ == "__main__":

    # Example watermark for testing.
    last_crash_date = "2026-10-08T00:00:00.000"

    # Calculate the start of a seven-day lookback.
    start_date = calculate_lookback_start(
        last_crash_date,
        days=7
    )

    print(f"Current watermark: {last_crash_date}")
    print(f"Lookback start: {start_date}")
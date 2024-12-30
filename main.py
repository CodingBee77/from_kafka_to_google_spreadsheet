import logging
from quixstreams import Application
from datetime import timedelta
import pygsheets


# TODO: define returning object Temperature using Pydantic

def initializer_fn(msg: dict) -> dict:
    temperature = msg['current']['temperature_2m']
    return {"open": temperature,
            "close": temperature,
            "high": temperature,
            "low": temperature}


def reducer_fn(summary: dict, msg: dict) -> dict:
    """

    Reducer function combines 2 given arguments into a new aggregated value and returns it.
    This aggregated value will be also returned as a result of the window.

    :param summary: previously aggregated value (the "aggregated" argument)
    :param msg: current value (the "value" argument)
    :return: new aggregated value
    """
    temperature = msg['current']['temperature_2m']
    return {
        "open": summary["open"],
        "close": temperature,
        "high": max(summary["high"], temperature),
        "low": min(summary["low"], temperature)
    }


def main():
    app = Application(broker_address="localhost:9092",
                      loglevel="DEBUG",
                      consumer_group="weather_to_google",
                      auto_offset_reset="earliest",
                      )
    input_topic = app.topic("weather_data_demo")
    sdf = app.dataframe(input_topic)

    # Build a pipeline on sdf
    # sdf.group_into_hourly_batches(...)
    sdf = sdf.tumbling_window(duration_ms=timedelta(hours=1))

    # sdf.summarize_that_hour(...)
    sdf = sdf.reduce(initializer=initializer_fn, reducer=reducer_fn)

    sdf = sdf.final()

    sdf = sdf.update(lambda msg: logging.debug("Got: %s", msg))
    google_api = pygsheets.authorize()
    workspace = google_api.open("Weather Sheet")
    sheet = workspace[0]
    sheet.update_values(
        "A1",
        [
            [
                "Start",
                "End",
                "Open",
                "High",
                "Low",
                "Close",
                "Date",
            ]
        ],
    )

    def to_google(msg):
        sheet.insert_rows(
            1,
            values=[
                msg["start"],
                msg["end"],
                msg["value"]["open"],
                msg["value"]["high"],
                msg["value"]["low"],
                msg["value"]["close"],
                "=EPOCHTODATE(A2 / 1000)",
            ],
        )

    sdf = sdf.apply(to_google)
    # sdf.send_to_google_sheets(...)
    app.run(sdf)


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()

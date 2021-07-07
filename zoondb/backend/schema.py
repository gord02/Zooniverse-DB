
import numpy as np
# import requests

from sanic_openapi import doc
import motor.motor_asyncio
import asyncio

# Create a new connection to a single MongoDB instance at host:port.
# client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
# # # Connection to a database 
# db = client['zooniverseDB']
# # Connect to collection in the DB zooniverseDB
# events = db['events']

class ZooniverseClassificationReport:
    event = doc.Integer("Event Number", required=True)
    beam = doc.Integer("beam number", required=True)
    ml_prediction = doc.Float("zooniverse ML prediction", required=True)
    retired = doc.Boolean("retired")
    t0_astro = doc.Float("t0_astro")
    t1_blank = doc.Float("t1_blank")
    t1_overlapping = doc.Float("t1_overlapping")
    t1_repeating = doc.Float("t1_repeating")
    t0_rfi = doc.Float("t0_rfi")
    t0_cant_answer = doc.Float("t0_cant-answer")
    t1_something_weird = doc.Float("t0_something-weird")
    t0_total = doc.Float("t0_total")
    t1_total = doc.Float("t1_total")
    t0_astro_fraction = doc.Float("t0_astro_fraction")
    t0_rfi_fraction = doc.Float("t0_rfi_fraction")
    t0_cant_answer_fraction = doc.Float("t0_cant-answer_fraction")
    t1_blank_fraction = doc.Float("t1_blank_fraction")
    t1_overlapping_fraction = doc.Float("t1_overlapping_fraction")
    t1_repeating_fraction = doc.Float("t1_repeating_fraction")
    t1_something_weird_fraction = doc.Float("t1_something-weird_fraction")


class Event:
    event = doc.Integer("Event Number", required=True)
    dm = doc.Float("DM", required=True)
    # snr = doc.Float("snr of the brightest beam", required=True)
    snr = doc.List( Float(), "snr values of beams", required=True)
    beams = [doc.Integer("beam number", required=True)]
    data_paths = doc.List()
    transfer_status = doc.String(
        "status of the transfer to zooniverse.",
        required=False,
        choices=["COMPLETE", "INCOMPLETE", "FAILED", "CLEANED"],
    )
    zooniverse_classification = doc.String(
        "classification of the event from zooniverse",
        required=False,
        choices=["GOOD", "BAD", "INCOMPLETE"],
    )
    expert_classification = doc.String(
        "classification of the event from tsars.",
        required=False,
        choices=["GOOD", "BAD", "INCOMPLETE"],
    )

# The function is used to create event inside database
async def createEvent():
    event_model: dict = {
        "event": 1,
        "dm": 123.4,
        "snr": 7.9,
        "beams": [123, 1123],
        "data_paths": {
            123: "path to b1",
            1123: "path to b2",
        },
        "transfer_status": "INCOMPLETE",
        "zooniverse_classification": "INCOMPLETE",
        "expert_classification": "INCOMPLETE",
    }

    # randomizes the event model so different events can populate the DB.
    for _ in range(1000):
        event_model["event"] = int(np.random.choice(range(9386707, 9396707)))
        event_model["dm"] = float(np.random.random() * 10000)
        event_model["snr"] = float(np.random.random() + 7.5)
        event_model["beams"] = [
            int(
                np.random.choice(
                    list(range(0, 256))
                    + list(range(1000, 1256))
                    + list(range(2000, 2256))
                    + list(range(3000, 3256))
                )
            )
            for _ in range(np.random.choice(range(1, 5)))
        ]
        event_model["data_paths"] = {}

        for beam in event_model["beams"]:
            event_model["data_paths"][beam] = f"path to {beam}"

        event_model["transfer_status"] = np.random.choice(
            ["INCOMPLETE"] * 100 + ["COMPLETE"] * 20 + ["CLEANED"] * 30 + ["FAILED"] * 10
        )
        event_model["zooniverse_classification"] = np.random.choice(
            ["INCOMPLETE"] * 100 + ["GOOD"] * 20 + ["BAD"] * 30
        )
        if event_model["zooniverse_classification"] in ["INCOMPLETE", "BAD"]:
            event_model["expert_classification"] = "INCOMPLETE"
        else:
            event_model["expert_classification"] = np.random.choice(
                ["INCOMPLETE", "GOOD", "BAD"]
            )
    event = event_model

# Runs createEvent function asynchronously 
loop = asyncio.get_event_loop()
loop.run_until_complete(createEvent())


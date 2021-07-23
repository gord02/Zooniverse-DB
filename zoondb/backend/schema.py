
import numpy as np
from datetime import date

from mongoengine import connect, Document, FileField, StringField
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

class Beams:
    snr = doc.Float("snr value", required=True)
    beam_number = doc.Integer("beam number", required=True)
    data_paths = doc.List()
class Event(Document):
    event = doc.Integer("Event Number", required=True)
    dm = doc.Float("DM", required=True)

    beams = [Beams()]

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
    # test_plots = ['/Users/gordon/Desktop/zooniverse-db/files_for_zoon/data/frb-archiver/plot1.png','/Users/gordon/Desktop/zooniverse-db/files_for_zoon/data/frb-archiver/plot2.png', '/Users/gordon/Desktop/zooniverse-db/files_for_zoon/data/frb-archiver/plot3.png']
    event_model: dict = {
       	"event_number": 9386707,
		"dm": 715.9,
		"snr": 15.9,
		"beams": 123,
		"data_path": {
			# # "1166": "/data/frb-archiver/2018/07/25/astro_9386707/intensity/processed/1166/9386707_1166_intensityML.npz",
			# # "0166": "/data/frb-archiver/2018/07/25/astro_9386707/intensity/processed/0166/9386707_0166_intensityML.npz",
            # "123": "/Users/gordon/Desktop/zooniverse-db/files_for_zoon/data/frb-archiver/9386707_1166_intensityML.png",
			# "1234": "/Users/gordon/Desktop/zooniverse-db/files_for_zoon/data/frb-archiver/9386707_0166_intensityML.png"
        } ,
        "transfer_status": "INCOMPLETE",
        "zooniverse_classification": "INCOMPLETE",
        "expert_classification": "INCOMPLETE",
    }

    # randomizes the event model so different events can populate the DB.
    for _ in range(3):
        event_model["event_number"] = int(np.random.choice(range(9386707, 9396707)))
        event_model["dm"] = float(np.random.random() * 10000)
        event_model["snr"] = float(np.random.random() + 7.5)
        event_model["beam"] = int(np.random.choice(range(2000, 3256)))
        # event_model["beams"] = [
        #     int(
        #         np.random.choice(
        #             list(range(0, 256))
        #             + list(range(1000, 1256))
        #             + list(range(2000, 2256))
        #             + list(range(3000, 3256))
        #         )
        #     )
        #     for _ in range(np.random.choice(range(1, 5)))
        # ]
        beam = event_model["beam"]
        event_num = event_model["event_number"]
        i = int(np.random.choice(range(0, 2)))
        n = int(np.random.choice(range(0, 2)))
        the_date = str(date.today())

        new_data_path_dict = {
            str(beam): "/data/chime/intensity/processed"+the_date+"astro_"+str(event_num)+"/"+str(beam)+"/"+str(event_num)+"_"+str(beam)+"_intensityML.png",
        }

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
        event_number = event["event_number"]
        dm_value = event["dm"]
        transfer_status =  event["transfer_status"],
        zooniverse_classification = event["zooniverse_classification"],
        expert_classification = event["expert_classification"]

        snr_value = event["snr"]
        beam_number = event["beam"]
        data_path = new_data_path_dict
        # data_path = event["data_path"]

        beams_dict = {
            "snr" : snr_value,
            "beam" : beam_number,
            "data_paths": data_path
        }
        document = {
            "event": event_number,
            "dm": dm_value, 
            # how are multiple beam dict supposed to end up here
            "beams": beams_dict, 
            "transfer_status": transfer_status,
            "zooniverse_classification": zooniverse_classification,
            "expert_classification":expert_classification
        }
        # Create a new connection to a single MongoDB instance at host:port.
        client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
        # Connection to a database 
        db = client['zooniverseDB']
        result = await client.zooniverseDB.events.insert_one(document)

# Runs createEvent function asynchronously 
loop = asyncio.get_event_loop()
loop.run_until_complete(createEvent())


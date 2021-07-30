"""Sample RESTful Framework."""
import json as json_converter

from sanic import Sanic
from sanic.request import Request
from sanic.response import HTTPResponse, json
# Blueprint is a way to simplify the process of routing
from sanic import Blueprint
# imports simple way of applying doc string through framework sanic
from sanic_openapi import doc

# allows Sanic and mongoDB to work together 
import motor.motor_asyncio
import asyncio

from zoondb.backend import schema

# NOTE: The URL Prefix for your backend has to be the name of the backend
blueprint = Blueprint("zoondb Backend", url_prefix="/")

@doc.consumes(schema.Event, schema.Beams)
@blueprint.post("transfer-event")
@doc.summary("CHIME/FRB event data is being sent to server so that it can be stored in zooniverse database")
async def get_event_data_from_CHIME(request):
    try:
        client = request.app.mongo.client
        
        req_data = request.args
        body = req_data["body"] 
        data = body[0]
        data_dict = json_converter.loads(data)

        event_number = data_dict["event"]
        dm_value = data_dict["dm"]

        snr_value = data_dict["snr"]
        beam_value = data_dict["beam_number"]
        data_path = data_dict["data_path"]

        
        # loop through for each beam value?
        beams_dict = {
            "snr" : snr_value,
            "beam" : beam_value,
            "data_paths": data_path
        }
        # code to add new event to database 
        document = {
            "event": event_number,
            "dm": dm_value, 
            # how are multiple beam dict supposed to end up here ??
            "beams": beams_dict, 
        }

        result = await client.zooniverseDB.events.insert_one(document)
        print("ID of added document %s" % repr(result.inserted_id))
        return  json(True)

    except Exception as error:
        print("error: ", str(error))
        return json(str(error))

@doc.summary("Fetch an event")
@doc.consumes(schema.Event)
@blueprint.get("event/<event_no>")
async def get_event(request, event_no):
    try:
        client = request.app.mongo.client
        items = []
        docs = client.zooniverseDB.events.find({"event": int(event_no)}, projection={"_id": 0})
        async for d in docs:
            items.append(d)
        return json(items)
    except Exception as e:
        print(str(e))
        raise

@doc.summary("Fetch all events")
@doc.consumes(schema.Event)
@blueprint.get("all-events")
async def getAllEvents(request):
    try:
        client = request.app.mongo.client
        # total_number_of_events = await client.zooniverseDB.events.count_documents({})
        items = []
        docs = client.zooniverseDB.events.find({})
        async for d in docs:
            items.append(d)
        return  json(True)
 
    except Exception as error:
        print(str(error))
        return json(str(error))
        
@doc.summary("Update event.")
@doc.consumes(schema.Event)
@blueprint.put("event/<event_no>")
async def update_event(request, event_no):
    print(request.json)
    try:
        client = request.app.mongo.client
        client.zooniverseDB.events.find_one_and_update(
            {"event": int(event_no)}, {"$set": request.json}, projection={"_id": 0}
        )
        return json(True)
    except Exception as e:
        print(str(e))
        raise

@doc.summary("Delete event")
@doc.consumes(schema.Event)
@blueprint.delete("event/<event_no>")
async def delete_event(request, event_no):
    try:
        client = request.app.mongo.client
        client.zooniverseDB.events.delete_one({"event": int(event_no)})
        return json(True)
    except Exception as e:
        print(str(e))
        raise

@doc.summary("Fetch events to transfer to zooniverse.")
@doc.produces([schema.Event])
@blueprint.get("events-for-transfer")
async def fetch_events_for_transfer(request):
    try:
        client = request.app.mongo.client
        items = []
        docs = client.zooniverseDB.events.find(
            {"transfer_status": {"$in": ["INCOMPLETE", "FAILED"]}},
            projection={"_id": 0},
        )
        async for d in docs:
            items.append(d)
        return json(items)
    except Exception as e:
        print(str(e))
        raise

@doc.summary("Fetch events to transfer to cleanup.")
@doc.produces([schema.Event])
@blueprint.get("events-for-cleanup")
async def fetch_events_for_cleanup(request):
    try:
        client = request.app.mongo.client
        items = []
        docs = client.zooniverseDB.events.find(
            {"transfer_status": "COMPLETE", "zooniverse_classification": "BAD"},
            projection={"_id": 0},
        )
        async for d in docs:
            items.append(d)
        return json(items)
    except Exception as e:
        print(str(e))
        raise

@doc.summary("Fetch events for expert verification.")
@doc.produces([schema.Event])
@blueprint.get("events-for-experts")
async def fetch_events_for_experts(request):
    try:
        client = request.app.mongo.client
        items = []
        docs = client.zooniverseDB.events.find(
            {
                "zooniverse_classification": "GOOD",
                "expert_classification": "INCOMPLETE",
            },
            projection={"_id": 0},
        )
        async for d in docs:
            items.append(d)
        return json(items)
    except Exception as e:
        print(str(e))
        raise

@doc.summary("Add zooniverse classification")
@blueprint.put("zooniverse-classification/<event_no>/<classification>")
async def zooniverse_classification(request, event_no, classification):
    assert classification.upper() in ["GOOD", "BAD", "INCOMPLETE"]
    try:
        client = request.app.mongo.client
        client.zooniverseDB.events.find_one_and_update(
            {"event": int(event_no)},
            {"$set": {"zooniverse_classification": classification.upper()}},
            projection={"_id": 0},
        )
        return json(True)
    except Exception as e:
        print(str(e))
        raise

@doc.summary("Add expert classification")
@blueprint.put("expert-classification/<event_no>/<classification>")
async def expert_classification(request, event_no, classification):
    assert classification.upper() in ["GOOD", "BAD", "INCOMPLETE"]
    try:
        client = request.app.mongo.client
        client.zooniverseDB.events.find_one_and_update(
            {"event": int(event_no)},
            {"$set": {"expert_classification": classification.upper()}},
            projection={"_id": 0},
        )
        return json(True)
    except Exception as e:
        print(str(e))
        raise




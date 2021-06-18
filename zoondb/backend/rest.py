"""Sample RESTful Framework."""
from sanic import Blueprint
from sanic.request import Request
from sanic.response import HTTPResponse, json
from sanic_openapi import doc


from zoondb.routines import composite, simple

# NOTE: The URL Prefix for your backend has to be the name of the backend
blueprint = Blueprint("zoondb Backend", url_prefix="/")


@doc.summary("Hello from zooniverse-db!")
@blueprint.get("hello")
async def hello(request: Request) -> HTTPResponse:
    """Hello World.

    Parameters
    ----------
    request : Request
        Request object from sanic app

    Returns
    -------
    HTTPResponse
    """
    return json("Hello from zooniverse-db 🦧")


@doc.summary("Seeder Routine")
@blueprint.get("simple")
async def get_seeder(request: Request) -> HTTPResponse:
    """Run Seeder Routine.

    Parameters
    ----------
    request : Request
        Request object from sanic app

    Returns
    -------
    HTTPResponse
    """
    example = simple.Simple('hex')
    return json(example.seedling())


@doc.summary("Composite Routine")
@blueprint.get("composite")
async def get_composite(request: Request) -> HTTPResponse:
    """Run Composite Routine.

    Parameters
    ----------
    request : Request
        Request object from sanic app

    Returns
    -------
    HTTPResponse
    """
    example = composite.Composite(1.0, 10.0, "hex")
    return json(example.get_random_integer())


# @doc.consumes(schema.Event)
@blueprint.post("transfer-event")
@doc.summary("CHIME/FRB event data is being sent to server so that it can be stored in zooniverse database")
async def get_event_data_from_CHIME(request):
    try:
        data = request.args
        event_number = data["event_number"]
        dm_value = data["dm"]
        snr_value = data["snr"]
        data_path = data["data_path"]
        return  json(True)
    #     # code to add to database here

    except Exception as error:
        print(str(error))
        return json(str(error))


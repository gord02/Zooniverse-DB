"""
Configuration for setting up Maestro with pytest 
"""

import os

from sanic import Sanic
import pytest
from maestro import Maestro
from maestro.core import configuration


config = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "../backend.yml"
)
config = configuration.generate(mode="development", path=config)
backend = Maestro(mode="development", config=config, debug=True)
test_app = backend.app


@pytest.fixture
def app():
    yield test_app

@pytest.fixture
def test_cli(loop, app, sanic_client):
    return loop.run_until_complete(sanic_client(app))

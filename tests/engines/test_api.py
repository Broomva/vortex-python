import base64
import json

import pytest
import responses
from requests import Session

from vortex.engine.api import APIEngine

# Test data
api_params = {
    "url": "https://api.example.com/data",
    "user": "test_user",
    "password": "test_password",
    "content_type": "application/json",
    "token_type": "Basic",
}


@pytest.fixture
def api_engine():
    return APIEngine(api_params)


@pytest.fixture
def expected_token():
    return base64.b64encode(
        f"{api_params['user']}:{api_params['password']}".encode("ascii")
    ).decode("ascii")


def test_create_token(api_engine, expected_token):
    assert (
        api_engine.create_token(api_params["user"], api_params["password"])
        == expected_token
    )


def test_set_headers(api_engine, expected_token):
    headers = api_engine.set_headers()
    assert "Authorization" in headers
    assert headers["Authorization"] == f"{api_params['token_type']} {expected_token}"
    assert "Content-Type" in headers
    assert headers["Content-Type"] == api_params["content_type"]


@responses.activate
def test_post_request(api_engine):
    responses.add(
        responses.POST, api_params["url"], json={"message": "Success"}, status=200
    )
    headers = api_engine.set_headers()
    session = Session()  # Add this line to create a new Session object
    response = api_engine.post_request(
        json.dumps({"key": "value"}), headers, session
    )  # Pass the session object instead of api_engine


@responses.activate
def test_post_request_error(api_engine):
    responses.add(
        responses.POST, api_params["url"], json={"error": "Bad Request"}, status=400
    )
    headers = api_engine.set_headers()
    session = Session()  # Add this line to create a new Session object
    response = api_engine.post_request(
        json.dumps({"key": "value"}), headers, session
    )  # Pass the session object instead of api_engine


@responses.activate
def test_get_request(api_engine):
    responses.add(
        responses.GET, api_params["url"], json={"data": [1, 2, 3]}, status=200
    )
    response = api_engine.get_request()
    assert response.status_code == 200
    assert response.json() == {"data": [1, 2, 3]}


@responses.activate
def test_get_request_error(api_engine):
    responses.add(
        responses.GET, api_params["url"], json={"error": "Not Found"}, status=404
    )
    response = api_engine.get_request()
    assert response.status_code == 404
    assert response.json() == {"error": "Not Found"}

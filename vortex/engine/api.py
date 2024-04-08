import base64
from json import dumps, loads
from time import time

from requests import Request, Session
from vortex.engine import Engine
from vortex.datamodels.engine import API
from vortex.utils.utils import get_iter_progress

import logging

logger = logging.getLogger(__name__)


class APIEngine(Engine):
    """
    It makes sense to use the Builder pattern only when your products are quite
    complex and require extensive configuration.

    Unlike in other creational patterns, different concrete builders can produce
    unrelated products. In other words, results of various builders may not
    always follow the same interface.
    """

    def __init__(self, params: dict) -> None:
        self._params = API(**params)

    def read_stream(self, spark):
        pass

    def write_stream(self, dataframe, stage, batch_id, app_id, spark) -> None:
        pass

    def read_batch(self, spark):
        pass

    def write_batch(self, dataframe, spark=None):
        headers = self.set_headers()
        errors = []
        session = Session()
        session.headers.update(headers)

        for i, val in enumerate(dataframe):
            start_time = time()
            response = self.post_request(dumps(loads(val)), headers, session)
            logger.info(
                f"API Insert ran with responses {response.status_code},{response.text} duration: {time() - start_time} seconds"
            )
            logger.info("----------------------------------------------------------")
            progress_fraction, progress_bar = get_iter_progress(dataframe, i, "=")
            logger.info(
                f"Site {i} from {len(dataframe)}, {progress_fraction * 100} percent"
            )
            logger.info(progress_bar)

            if response.status_code != 200:
                errors.append(
                    {
                        "code": response.status_code,
                        "message": response.text,
                        "payload": val,
                    }
                )
        if len(errors) > 0:
            logger.error(f"Found Errors on: {errors}")
            # raise Exception('Error sending data to Api')

    def get_token(self):
        pass

    def create_token(self, user: str, password: str):
        token = f"{user}:{password}"
        encode_token = token.encode("ascii")
        token = base64.b64encode(encode_token).decode("ascii")
        return token

    def set_headers(self):
        headers = {}
        if self._params.additional_headers:
            headers = self._params.additional_headers

        if not self._params.token:
            token = self.create_token(self._params.user, self._params.password)
        else:
            token = self._params.token

        token = {
            "Authorization": f"{self._params.token_type} {token}",
        }

        headers.update(token)

        if self._params.content_type:
            content_type = {
                "Content-Type": f"{self._params.content_type}",
            }
            headers.update(content_type)
        return headers

    def post_request(self, payload, headers, session):
        req = Request("POST", self._params.url, headers=headers, data=payload)
        prepped = session.prepare_request(req)
        response = session.send(prepped)
        return response

    def get_request(self, payload=None, headers=None, session=None):
        req = Request("GET", self._params.url, headers=headers, data=payload)
        if not session:
            session = Session()
        if not headers:
            session.headers.update(self.set_headers())
        if headers:
            session.headers.update(headers)
        prepped = session.prepare_request(req)
        response = session.send(prepped)
        return response

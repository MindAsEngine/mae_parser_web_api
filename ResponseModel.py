import http
from http.client import OK


class ResponseModel:
    def __init__(self, data_length: int = None, data: list = None, status_code: http.HTTPStatus = OK):
        self.status_code = status_code
        self.data_length = data_length
        self.data = data

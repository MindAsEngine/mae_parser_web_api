import http
from http.client import OK


class ResponseModel:
    def __init__(self, data_length: int = None, data: object = None, status_code: http.HTTPStatus = OK,
                 active_data_len: int = 0):
        self.status_code = status_code
        self.data_length = data_length
        # data = None
        self.active_data_len = active_data_len
        self.data = data  # todo recursionl



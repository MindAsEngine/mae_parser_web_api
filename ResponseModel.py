class ResponseModel:
    def __init__(self, data_length: int, data: list, status_code: str):
        self.data_length = data_length
        self.data = data
        self.status_code = status_code
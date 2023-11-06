from datetime import datetime
from pydantic import BaseModel


class Flat(BaseModel):
    external_id: int
    url: str
    square: float
    floor: str
    total_floor: str
    address: str
    repair: str
    is_new_building: str
    room: str
    modified: datetime
    price_uye: float
    price_uzs: float
    description: str
    domain: str
    is_active: bool

    class Config:
        orm_mode = True

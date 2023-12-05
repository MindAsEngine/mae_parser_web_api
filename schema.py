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
    # modified: str
    price_uye: float
    price_uzs: float
    description: str
    domain: str
    is_active: bool

    class Config:
        orm_mode = True


class Land(BaseModel):
    external_id: int
    domain: str
    url: str
    square: float
    address: str
    location_feature: str
    type_of_land: str
    price_uye: float
    price_uzs: float
    description: str
    modified: datetime
    # modified: str
    is_active: bool

    class Config:
        orm_mode = True


class Commerce(BaseModel):
    external_id: int
    url: str
    domain: str
    square: float
    address: str
    type_of_commerce: str
    price_uye: float
    price_uzs: float
    description: str
    modified: datetime
    # modified: str
    is_active: bool

    class Config:
        orm_mode = True

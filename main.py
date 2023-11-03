from datetime import datetime
import uvicorn
from fastapi import FastAPI
from fastapi_sqlalchemy import DBSessionMiddleware, db

from schema import Flat as SchemaFlat

from models import Flat as ModelFlat

from ResponseModel import ResponseModel

import os
from dotenv import load_dotenv

load_dotenv('.env')

app = FastAPI()

# to avoid csrftokenError
app.add_middleware(DBSessionMiddleware, db_url=os.environ['DATABASE_URL'])


@app.get("/get_flats")
async def get_all(
        page: int = 0,
        limit: int = 100,
        domain: str = "olx"):
    data = list(db.session
                .query(ModelFlat)
                .filter_by(domain=domain, is_active=True)
                .order_by(ModelFlat.id)
                .slice(page * limit, (page + 1) * limit))

    for i in data:
        print(i)

    return ResponseModel(
        data_length=len(data),
        data=data
    )


@app.post("/post_flat")
async def post_flat(request: list[SchemaFlat]):
    for flat in request:
        db_flat = ModelFlat(
            external_id=flat.id,
            url=flat.url,
            square=flat.square,
            floor=flat.floor,
            total_floor=flat.total_floor,
            address=flat.address,
            repair=flat.repair,
            is_new_building=flat.is_new_building,
            room=flat.room,
            modified=flat.modified,
            price_uye=flat.price_uye,
            price_uzs=flat.price_uzs,
            description=flat.description,
            domain=flat.domain,
            is_active=flat.is_active
        )
        if db.session.query(ModelFlat).filter_by(external_id=flat.id).count() == 0:
            db.session.add(db_flat)
        else:
            db.session.merge(db_flat)
    return db.session.commit()

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)

import asyncio
from datetime import datetime
import uvicorn
from fastapi import FastAPI
from fastapi_sqlalchemy import DBSessionMiddleware, db
import logging

from schema import Flat as SchemaFlat

from models import Flat as ModelFlat

from ResponseModel import ResponseModel

import os
from dotenv import load_dotenv

load_dotenv('.env')

app = FastAPI()
app.fifo_queue = asyncio.Queue()
# to avoid csrftokenError
app.add_middleware(DBSessionMiddleware, db_url=os.environ['DATABASE_URL'])


@app.get("/get_flats")
async def get_all(
        page: int = 0,
        limit: int = 100,
        domain: str = "olx"):
    return get_limit_by_domain(page, limit, domain)


@app.post("/post_flats")
async def post_flats(request: list[SchemaFlat]):
    for i in request:
        await app.fifo_queue.put(i)
    pass


@app.on_event("startup")
async def start_db():
    print("I am alive!")
    asyncio.create_task(worker())


async def worker():
    while True:

        flat = await app.fifo_queue.get()
        print(f"Processing {flat}")
        await save_flat(flat)


async def save_flat(flat: SchemaFlat):
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
    with db():
        if db.session.query(ModelFlat).filter_by(external_id=flat.id).count() == 0:
            db.session.add(db_flat)
        else:
            db.session.merge(db_flat)
        db.session.commit()
    pass


async def get_limit_by_domain(
        page: int = 0,
        limit: int = 100,
        domain: str = "olx"):
    print("I do not return I am a bad bitch!! Punish me sir!")
    return ResponseModel(
        status_code="Ok",
        data_length=db.session.query(ModelFlat).filter_by(domain=domain).count(),
        data=list(db.session
                  .query(ModelFlat)
                  .filter_by(domain=domain, is_active=True)
                  .order_by(ModelFlat.id)
                  .slice(page * limit, (page + 1) * limit)))


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)

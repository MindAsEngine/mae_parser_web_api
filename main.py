import asyncio
import http

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
# logging.basicConfig(filename=os.environ['LOG_FILE'],
#                     level=logging.DEBUG)
# logger = logging.getLogger(__name__)

# Create a logger
logger = logging.getLogger('alembic')
logger.setLevel(logging.DEBUG)

# Create a file handler to redirect log output to a file
file_handler = logging.FileHandler(os.environ['LOG_FILE'])
file_handler.setLevel(logging.INFO)

# Create a formatter and add it to the file handler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)
logger.addHandler(consoleHandler)

status_codes = http.HTTPStatus
# Pass the logger to alembic.config.main() as the logger argument


@app.get("/get_flats")
async def get_all(
        page: int = 0,
        limit: int = 10000,
        domain: str = "uybor"):
    return ResponseModel(
        data_length=db.session.query(ModelFlat).filter_by(domain=domain).count(),
        data=list(db.session
                  .query(ModelFlat)
                  .filter_by(domain=domain, is_active=True)
                  .order_by(ModelFlat.external_id)
                  .slice(page * limit, (page + 1) * limit)))


@app.post("/post_flats")
async def post_flats(request: list[SchemaFlat]):
    if len(request) == 0:
        logger.warning(f'Post method received no data!')
        return ResponseModel(status_code=status_codes.NO_CONTENT)
    for i in request:
        await app.fifo_queue.put(i)
    return ResponseModel(status_code=status_codes.CONTINUE)


@app.get("/get_count")
async def get_count_by_domain(domain: str = ""):
    if domain == "":
        db_power = db.session.query(ModelFlat).count()
    else:
        db_power = db.session.query(ModelFlat).filter_by(domain=domain).count()
    logger.info(f'Total domain:{domain} length: {db_power}')
    return ResponseModel(data_length=db_power)


@app.on_event("startup")
async def start_db():
    asyncio.create_task(worker())


async def worker():
    while True:
        flat = await app.fifo_queue.get()
        logger.info(f"Processing id: {flat.id} with domain: {flat.domain}")
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
            logger.info(f'Saving entity:{db_flat.external_id} ADDED')
        else:
            db.session.merge(db_flat)
            logger.info(f'Updating entity: {db_flat.external_id} MERGED')
        db.session.commit()
        logger.info(f'OK! Committed total uybor: {db.session.query(ModelFlat).filter_by(domain="uybor").count()}')
        logger.info(f'OK! Committed total olx: {db.session.query(ModelFlat).filter_by(domain="olx").count()}')
    pass


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
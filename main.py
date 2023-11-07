import asyncio
import http

import uvicorn
from fastapi import FastAPI
from fastapi_sqlalchemy import DBSessionMiddleware, db
import logging

from schema import Flat as SchemaFlat, GetFlatsMessage

from models import Flat as ModelFlat

from ResponseModel import ResponseModel

import os
from dotenv import load_dotenv

load_dotenv('.env')

app = FastAPI()
app.fifo_queue = asyncio.Queue()
app.response_queue = asyncio.Queue()
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
    await app.fifo_queue.put(GetFlatsMessage(page=page, domain=domain, limit=limit))
    return await app.response_queue.get()


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
    asyncio.create_task(worker(app.fifo_queue))
    asyncio.create_task(consumer(app.response_queue))


async def consumer(response_queue: asyncio.Queue):
    while True:
        message = await response_queue.get()
        if isinstance(message, GetFlatsMessage):
            logger.info(f"Processing command: {message}")
            return await read_flat(
                page=message.page,
                limit=message.limit,
                domain=message.domain)
        app.response_queue.task_done()


async def worker(fifo_queue: asyncio.Queue):
    while True:
        message = await fifo_queue.get()
        logger.info(message is SchemaFlat)
        logger.info(message is GetFlatsMessage)
        if isinstance(message, SchemaFlat):
            logger.info(f"Processing id: {message.external_id} with domain: {message.domain}")
            await save_flat(message)
        elif isinstance(message, GetFlatsMessage):
            logger.info(f"Processing command: {message}")
            await app.response_queue.put(message)
        elif isinstance(message, ResponseModel):
            logger.info(f"Responce is here {message}")
        else:
            logger.info(f'Worker exited')
        app.fifo_queue.task_done()


async def read_flat(
        page: int = 0,
        limit: int = 10000,
        domain: str = "uybor"):
    with db():
        data = db.session.query(ModelFlat).filter_by(domain=domain, is_active=True).order_by(ModelFlat.external_id).slice(page * limit, (page + 1) * limit)
        data_len = db.session.query(ModelFlat).filter_by(domain=domain).count()
    logger.info(f'Read from db {data_len}')
    return ResponseModel(
            data_length=data_len,
            data=data)


async def save_flat(flat: SchemaFlat):
    db_flat = ModelFlat(
        external_id=flat.external_id,
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
        query = db.session.query(ModelFlat).filter_by(external_id=db_flat.external_id, domain=db_flat.domain)
        time_format = "%d/%m/%Y %H:%M:%S"
        modified_db = None
        modified_request = None

        if query.first() is not None:
            modified_db = query.first().modified
            modified_request = flat.modified
            logger.info(
                f'Date time for merge equation \n{modified_db.strftime(time_format)}\n{modified_request.strftime(time_format)}')
        if query.count() == 0:
            db.session.add(db_flat)
            logger.info(f'Saving entity:{db_flat.external_id} ADDED')
        elif modified_db.strftime(time_format) != modified_request.strftime(time_format):
            db.session.merge(db_flat)
            logger.error(
                f'Updating entity: {db_flat.external_id} MERGED with modeified:\n{modified_db.strftime(time_format)}\n{modified_request.strftime(time_format)}')
        else:
            logger.warning(f'Continuing with no save {db_flat.external_id} {db_flat.domain}')
        db.session.commit()

        logger.info(f'OK! Committed total uybor: {db.session.query(ModelFlat).filter_by(domain="uybor").count()}')
        logger.info(f'OK! Committed total olx: {db.session.query(ModelFlat).filter_by(domain="olx").count()}')
    pass


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)

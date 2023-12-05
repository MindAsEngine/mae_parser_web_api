import asyncio
import datetime
import http
import random

import aiohttp
import uvicorn
from fastapi import FastAPI
from fastapi_sqlalchemy import DBSessionMiddleware, db
import logging

from schema import Flat as SchemaFlat
from schema import Land as SchemaLand
from schema import Commerce as SchemaCommerce

from models import Flat as ModelFlat
from models import Land as ModelLand
from models import Commerce as ModelCommerce

from ResponseModel import ResponseModel

import os
from dotenv import load_dotenv

load_dotenv('.env')

app = FastAPI()
app.response_queue = asyncio.Queue()
app.priority_queue = asyncio.Queue()
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
async def get_all_flats(
        page: int = 0,
        limit: int = 10000,
        domain: str = "uybor"):
    response = await read_flat(page=page, limit=limit, domain=domain)
    return response


@app.get("/get_lands")
async def get_all_lands(
        page: int = 0,
        limit: int = 10000,
        domain: str = "uybor"):
    response = await read_land(page=page, limit=limit, domain=domain)
    return response


@app.get("/get_commerces")
async def get_all_commerces(
        page: int = 0,
        limit: int = 10000,
        domain: str = "uybor"):
    response = await read_commerce(page=page, limit=limit, domain=domain)
    return response


async def read_flat(
        page: int = 0,
        limit: int = 10000,
        domain: str = "uybor"):
    with db():
        data = list(db.session.query(ModelFlat).filter_by(domain=domain, is_active=True).order_by(
            ModelFlat.external_id).slice(page * limit, (page + 1) * limit))
        data_len = db.session.query(ModelFlat).filter_by(domain=domain).count()
        active_data_len = db.session.query(ModelFlat).filter_by(domain=domain, is_active=True).count()
    logger.info(f'Read active flats from {domain}: {active_data_len} total: {data_len} flats')
    return ResponseModel(
        data_length=data_len, #ALL_DATA
        active_data_len=active_data_len, #active_data_length
        data=data)


async def read_land(
        page: int = 0,
        limit: int = 10000,
        domain: str = "uybor"):
    with db():
        data = list(db.session.query(ModelLand).filter_by(domain=domain, is_active=True).order_by(
            ModelLand.external_id).slice(page * limit, (page + 1) * limit))
        data_len = db.session.query(ModelLand).filter_by(domain=domain).count()
        active_data_len = db.session.query(ModelLand).filter_by(domain=domain, is_active=True).count()
    logger.info(f'Read active lands from {domain}: {active_data_len} total: {data_len} flats')
    return ResponseModel(
        data_length=data_len, #ALL_DATA
        active_data_len=active_data_len, #active_data_length
        data=data)


async def read_commerce(
        page: int = 0,
        limit: int = 10000,
        domain: str = "uybor"):
    with db():
        data = list(db.session.query(ModelCommerce).filter_by(domain=domain, is_active=True).order_by(
            ModelCommerce.external_id).slice(page * limit, (page + 1) * limit))
        data_len = db.session.query(ModelCommerce).filter_by(domain=domain).count()
        active_data_len = db.session.query(ModelCommerce).filter_by(domain=domain, is_active=True).count()
    logger.info(f'Read active commerces from {domain}: {active_data_len} total: {data_len} flats')
    return ResponseModel(
        data_length=data_len, #ALL_DATA
        active_data_len=active_data_len, #active_data_length
        data=data)


@app.post("/post_flats")
async def post_flats(request: list[SchemaFlat]):
    if len(request) == 0:
        logger.warning(f'Post method received no data!')
        return ResponseModel(status_code=status_codes.NO_CONTENT)
    for i in request:
        await save_flat(i)
        # await app.response_queue.put(i)
    return ResponseModel(status_code=status_codes.CONTINUE)


@app.post("/post_lands")
async def post_lands(request: list[SchemaLand]):
    if len(request) == 0:
        logger.warning(f'Post method received no data!')
        return ResponseModel(status_code=status_codes.NO_CONTENT)
    for i in request:
        await save_land(i)
        # await app.response_queue.put(i)
    return ResponseModel(status_code=status_codes.CONTINUE)


@app.post("/post_commerces")
async def post_commerces(request: list[SchemaCommerce]):
    if len(request) == 0:
        logger.warning(f'Post method received no data!')
        return ResponseModel(status_code=status_codes.NO_CONTENT)
    for i in request:
        await save_commerce(i)
        # await app.response_queue.put(i)
    return ResponseModel(status_code=status_codes.CONTINUE)


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
        time_format = "%d/%m/%Y %H:%M:%S.%f"
        modified_db = None
        modified_request = None

        if query.first() is not None:
            modified_db = query.first().modified  # datetime.datetime.strptime(, time_format)
            modified_request = flat.modified
            logger.info(f'Flat has been found')
        if query.count() == 0:
            db.session.add(db_flat)
        elif modified_db.minute != modified_request.minute or modified_db.second != modified_request.second or modified_db.microsecond != modified_request.microsecond:
            db.session.merge(db_flat)
            logger.info(
                f'Updating entity: {db_flat.external_id} MERGED with modified:{modified_db.strftime(time_format)}\n{modified_request.strftime(time_format)}')
        else:
            logger.info(f'Continuing with no save {db_flat.external_id} {db_flat.domain}')
        db.session.commit()

        logger.info(f'Committed total uybor: {db.session.query(ModelFlat).filter_by(domain="uybor").count()}')
        logger.info(f'Committed total olx: {db.session.query(ModelFlat).filter_by(domain="olx").count()}')

        logger.info(f'Active uybor: {db.session.query(ModelFlat).filter_by(domain="uybor", is_active=True).count()}')
        logger.info(f'Active olx: {db.session.query(ModelFlat).filter_by(domain="olx", is_active=True).count()}')
    pass


async def save_land(land: SchemaLand):
    db_land = ModelLand(
        external_id=land.external_id,
        domain=land.domain,
        url=land.url,
        square=land.square,
        address=land.address,
        location_feature=land.location_feature,
        type_of_land=land.type_of_land,
        price_uye=land.price_uye,
        price_uzs=land.price_uzs,
        description=land.description,
        modified=land.modified,
        is_active=land.is_active,
    )
    with db():
        query = db.session.query(ModelLand).filter_by(external_id=db_land.external_id, domain=db_land.domain)
        time_format = "%d/%m/%Y %H:%M:%S.%f"
        modified_db = None
        modified_request = None

        if query.first() is not None:
            modified_db = query.first().modified  # datetime.datetime.strptime(, time_format)
            modified_request = land.modified
            logger.info(f'land has been found')
        if query.count() == 0:
            db.session.add(db_land)
        elif modified_db.minute != modified_request.minute or modified_db.second != modified_request.second or modified_db.microsecond != modified_request.microsecond:
            db.session.merge(db_land)
            logger.info(
                f'Updating entity: {db_land.external_id} MERGED with modified:{modified_db.strftime(time_format)}\n{modified_request.strftime(time_format)}')
        else:
            logger.info(f'Continuing with no save {db_land.external_id} {db_land.domain}')
        db.session.commit()

        logger.info(f'Committed total uybor: {db.session.query(ModelLand).filter_by(domain="uybor").count()}')
        logger.info(f'Committed total olx: {db.session.query(ModelLand).filter_by(domain="olx").count()}')

        logger.info(f'Active uybor: {db.session.query(ModelLand).filter_by(domain="uybor", is_active=True).count()}')
        logger.info(f'Active olx: {db.session.query(ModelLand).filter_by(domain="olx", is_active=True).count()}')
    pass


async def save_commerce(commerce: SchemaCommerce):
    db_commerce = ModelCommerce(
        external_id =commerce.external_id,
        url = commerce.url,
        domain = commerce.domain,
        square = commerce.square,
        address = commerce.address,
        type_of_commerce = commerce.type_of_commerce,
        price_uye = commerce.price_uye,
        price_uzs = commerce.price_uzs,
        description = commerce.description,
        modified = commerce.modified,
        is_active = commerce.is_active,
    )
    with db():
        query = db.session.query(ModelCommerce).filter_by(external_id=db_commerce.external_id, domain=db_commerce.domain)
        time_format = "%d/%m/%Y %H:%M:%S.%f"
        modified_db = None
        modified_request = None

        if query.first() is not None:
            modified_db = query.first().modified  # datetime.datetime.strptime(, time_format)
            modified_request = commerce.modified
            logger.info(f'commerce has been found')
        if query.count() == 0:
            db.session.add(db_commerce)
        elif modified_db.minute != modified_request.minute or modified_db.second != modified_request.second or modified_db.microsecond != modified_request.microsecond:
            db.session.merge(db_commerce)
            logger.info(
                f'Updating entity: {db_commerce.external_id} MERGED with modified:{modified_db.strftime(time_format)}\n{modified_request.strftime(time_format)}')
        else:
            logger.info(f'Continuing with no save {db_commerce.external_id} {db_commerce.domain}')
        db.session.commit()

        logger.info(f'Committed total uybor: {db.session.query(ModelCommerce).filter_by(domain="uybor").count()}')
        logger.info(f'Committed total olx: {db.session.query(ModelCommerce).filter_by(domain="olx").count()}')

        logger.info(f'Active uybor: {db.session.query(ModelCommerce).filter_by(domain="uybor", is_active=True).count()}')
        logger.info(f'Active olx: {db.session.query(ModelCommerce).filter_by(domain="olx", is_active=True).count()}')
    pass


@app.get("/get_count")
async def get_count_by_domain(domain: str = ""):
    if domain == "":
        db_power = db.session.query(ModelFlat).count()
    else:
        db_power = db.session.query(ModelFlat).filter_by(domain=domain).count()
    logger.info(f'Total domain:{domain} length: {db_power}')
    return ResponseModel(data_length=db_power)


@app.get("/is_active")
async def is_active_get(wrong_type_of_market: bool = False):
    offers = db.session.query(ModelFlat).all()
    await is_active_all_offers(offers, wrong_type_of_market)
    db_power = db.session.query(ModelFlat).filter_by(is_active=True).count()
    return ResponseModel(data_length=db_power)


async def is_active_all_offers(offers, wrong_type_of_market=False):
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36 OPR/60.0.3255.170",
        "accept": "*/*"
    }
    for offer in offers:
        while True:
            session_timeout = aiohttp.ClientTimeout(total=None)
            await asyncio.sleep(random.random())
            session = aiohttp.ClientSession(headers=headers, timeout=session_timeout)
            if offer.domain == "uybor":
                url = f'https://api.uybor.uz/api/v1/listings/{offer.external_id}'
                async with (session.get(url) as resp):
                    if resp.status == 200:
                        # print(url)
                        data = await resp.json()
                        if not data.get('isActive'):
                            offer.is_active = False
                            db.session.merge(offer)
                            db.session.commit()
                            logger.info(f'Set to inactive: {offer.external_id} domain: {offer.domain}')
                    elif resp.status == 404 or resp.status == 410:
                        offer.is_active = False
                        db.session.merge(offer)
                        db.session.commit()
                        logger.info(f'Not found status set to inactive: {offer.external_id} domain: {offer.domain}')
            elif offer.domain == "olx":
                url = f'https://www.olx.uz/api/v1/offers/{offer.external_id}'
                async with (session.get(url) as resp):
                    if resp.status == 200:
                        # print(url)
                        response = await resp.json()
                        data = response.get('data')
                        if data.get('status') != 'active':
                            offer.is_active = False
                            db.session.merge(offer)
                            db.session.commit()
                            logger.info(f'Set to inactive: {offer.external_id} domain: {offer.domain}')
                        if wrong_type_of_market:
                            params = data.get('params')
                            for param in params:
                                key = param.get("key")
                                if key == 'type_of_market':
                                    if param.get('value').get('key') == 'secondary':
                                        type_of_market = "Вторичный"
                                    elif param.get('value').get('key') == 'primary':
                                        type_of_market = "Новостройка"
                                    else:
                                        logger.error(f"Bad format type of market {param.get('value').get('key')}")
                                        break
                                    offer.is_new_building = type_of_market
                                    db.session.merge(offer)
                                    db.session.commit()
                                    logger.info(
                                        f'Wrong type of market for {offer.domain} {offer.external_id} set: {type_of_market}')
                                    break
                                logger.error(f'Nothing happend to {offer.external_id} domain: {offer.domain}')
                    elif resp.status == 404 or resp.status == 410:
                        offer.is_active = False
                        db.session.merge(offer)
                        db.session.commit()
                        logger.info(f'Not found status set to inactive: {offer.external_id} domain: {offer.domain}')
                    elif resp.status == 403:
                        await asyncio.sleep(random.randint(5, 15))
                        continue
                    else:
                        logger.warning(f'Response not handled {resp.status} in domain: {offer.domain}')
            else:
                logger.info(f'This domain was never used')
            await session.close()
            logger.info(f'Progress {(offers.index(offer)/len(offers))*100}%')
            break


# @app.on_event("startup")
# async def start_db():
#     # await asyncio.gather(worker(app.fifo_queue), consumer(app.response_queue))
#     asyncio.create_task(worker())
#     asyncio.create_task(consumer())


# async def consumer():  # read from que
#     while True:
#         message = await app.priority_queue.get()
#         if isinstance(message, GetFlatsMessage):
#             logger.info(f"Processing command: {message}")
#             answer = await read_flat(
#                 page=message.page,
#                 limit=message.limit,
#                 domain=message.domain)
#             # app.priority_queue.task_done()
#             logger.warning(f'{answer}')
#             return answer
#
#
# async def worker():  # write
#     while True:
#         message = await app.response_queue.get()
#         logger.info(message is SchemaFlat)
#         logger.info(message is GetFlatsMessage)
#         if isinstance(message, SchemaFlat):
#             logger.info(f"Processing id: {message.external_id} with domain: {message.domain}")
#             await save_flat(message)
#         elif isinstance(message, GetFlatsMessage):
#             logger.info(f"Processing command: {message}")
#             await app.priority_queue.put(message)
#         elif isinstance(message, ResponseModel):
#             logger.info(f"Responce is here {message}")
#         else:
#             logger.info(f'Worker exited')
#         # app.response_queue.task_done()








if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)

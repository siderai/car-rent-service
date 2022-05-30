import asyncio
from asyncio import Queue

AGGREGATOR_REQUEST = [
    "somewhere1",
    "somewhere2",
    "somewhere3",
    "somewhere4",
]


def gen_offers(source: str) -> list[dict]:
    return [
        {"url": f"http://{source}/car?id=1", "price": 1_000, "brand": "LADA"},
        {"url": f"http://{source}/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
        {"url": f"http://{source}/car?id=3", "price": 3_000, "brand": "KIA"},
        {"url": f"http://{source}/car?id=4", "price": 2_000, "brand": "DAEWOO"},
        {"url": f"http://{source}/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]


def gen_agg_response():
    res = list()
    for s in AGGREGATOR_REQUEST:
        res.extend(gen_offers(s))

    return res


AGGREGATOR_RESPONSE = gen_agg_response()


def run_chain(chain, inbount: Queue, **kw) -> Queue:
    outbound = Queue()
    asyncio.create_task(chain(inbount, outbound, **kw))
    return outbound

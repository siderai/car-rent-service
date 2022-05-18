import asyncio
from asyncio import FIRST_COMPLETED, Event, Queue, Semaphore
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from app.const import MAX_PARALLEL_AGG_REQUESTS_COUNT, WORKERS_COUNT


class PipelineContext:

    def __init__(self, user_id: int, data: Optional[Any] = None):
        self._user_id = user_id
        self.data = data

    @property
    def user_id(self):
        return self._user_id


CURRENT_AGG_REQUESTS_COUNT = 0
BOOKED_CARS: Dict[int, Set[str]] = defaultdict(set)


async def get_offers(source: str) -> List[dict]:
    """
    Эта функция эмулирует асинхронный запрос по сети в сервис каршеринга source.
    Например source = "yandex" - запрашиваем список свободных автомобилей в сервисе yandex.
    """
    await asyncio.sleep(1)
    return [
        {
            "url": f"http://{source}/car?id=1",
            "price": 1_000,
            "brand": "LADA"
        },
        {
            "url": f"http://{source}/car?id=2",
            "price": 5_000,
            "brand": "MITSUBISHI"
        },
        {
            "url": f"http://{source}/car?id=3",
            "price": 3_000,
            "brand": "KIA"
        },
        {
            "url": f"http://{source}/car?id=4",
            "price": 2_000,
            "brand": "DAEWOO"
        },
        {
            "url": f"http://{source}/car?id=5",
            "price": 10_000,
            "brand": "PORSCHE"
        },
    ]


async def get_offers_from_sourses(sources: List[str]) -> List[dict]:
    """
    Эта функция агрегирует предложения из списка сервисов по каршерингу
    """

    global CURRENT_AGG_REQUESTS_COUNT
    if CURRENT_AGG_REQUESTS_COUNT >= MAX_PARALLEL_AGG_REQUESTS_COUNT:
        await asyncio.sleep(10.0)

    CURRENT_AGG_REQUESTS_COUNT += 1
    responces = await asyncio.gather(
        *[get_offers(source) for source in sources])
    CURRENT_AGG_REQUESTS_COUNT -= 1

    out = list()
    for responce in responces:
        out += responce
    return out


async def worker_combine_service_offers(inbound: Queue[PipelineContext],
                                        outbound: Queue[PipelineContext],
                                        requests_sem: Semaphore):
    """
    Этот воркер обрабатывает данные из очереди inbound и передает результат в очередь outbound.
    Количество параллельных вызовов функции get_offers_from_sourses в воркерах ограничено.
    """
    while True:
        async with requests_sem:
            ctx: PipelineContext = await inbound.get()
            ctx.data = await get_offers_from_sourses(ctx.data)
            await outbound.put(ctx)


async def chain_combine_service_offers(inbound: Queue[PipelineContext],
                                       outbound: Queue[PipelineContext], **kw):
    """
    Запускает N функций worker-ов для обработки данных из очереди inbound и передачи результата в outbound очередь.
    N worker-ов == WORKERS_COUNT (константа из app/const.py)

    Чтобы не перегружать внешний сервис, число параллельных вызовов get_offers_from_sourses для N воркеров ограничено.
    Ограничение количества вызовов == MAX_PARALLEL_AGG_REQUESTS_COUNT (константа из app/const.py)
    """
    requests_sem = Semaphore(MAX_PARALLEL_AGG_REQUESTS_COUNT)
    for _ in range(WORKERS_COUNT):
        asyncio.create_task(
            worker_combine_service_offers(inbound, outbound, requests_sem))


async def chain_filter_offers(
    inbound: Queue,
    outbound: Queue,
    brand: Optional[str] = None,
    price: Optional[int] = None,
    **kw,
):
    """
    Функция обработывает данных из очереди inbound и передает результат в outbound очередь.
    При наличии параметров brand и price - отфильтрует список предложений.
    """
    while True:
        ctx: PipelineContext = await inbound.get()
        filtered_offers = list()
        for offer in ctx.data:
            if brand:
                if brand != offer['brand']:
                    continue
            if price:
                if price < offer['price']:
                    continue
            filtered_offers.append(offer)
        ctx.data = filtered_offers

        await outbound.put(ctx)


async def cancel_book_request(user_id: int, offer: dict):
    """
    Эмулирует запрос отмены бронирования авто
    """
    await asyncio.sleep(1)
    BOOKED_CARS[user_id].remove(offer.get("url"))


async def book_request(user_id: int, offer: dict, event: Event) -> dict:
    """
    Эмулирует запрос бронирования авто. В случае отмены вызывает cancel_book_request.
    """
    try:
        BOOKED_CARS[user_id].add(offer.get("url"))
        await asyncio.sleep(1)
        if event.is_set():
            event.clear()
        else:
            event.wait()

        return offer

    except asyncio.CancelledError:
        await cancel_book_request(user_id, offer)


async def worker_book_car(inbound: Queue[PipelineContext],
                          outbound: Queue[PipelineContext]):
    """
    Этот worker обрабатывает данные из очереди inbound и передает результат в очередь outbound
    """
    while True:
        ctx: PipelineContext = await inbound.get()
        event = Event()
        event.set()
        tasks = list()
        for offer in ctx.data:
            task = asyncio.create_task(book_request(ctx.user_id, offer, event))
            tasks.append(task)

        # дожидаемся выполнения первой выполеннной задачи (бронирование), остальные отменяем
        done, pending = await asyncio.wait(tasks, return_when=FIRST_COMPLETED)

        for task in done:
            ctx.data = task.result()

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending)

        await outbound.put(ctx)


async def chain_book_car(inbound: Queue[PipelineContext],
                         outbound: Queue[PipelineContext], **kw):

    for _ in range(WORKERS_COUNT):
        asyncio.create_task(worker_book_car(inbound, outbound))


def run_pipeline(inbound: Queue[PipelineContext]) -> Queue[PipelineContext]:

    queue_1 = Queue()
    queue_2 = Queue()
    outbound = Queue()

    asyncio.create_task(chain_combine_service_offers(inbound, queue_1))
    asyncio.create_task(chain_filter_offers(queue_1, queue_2))
    asyncio.create_task(chain_book_car(queue_2, outbound))

    return outbound

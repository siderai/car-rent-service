import asyncio
import time
from asyncio import Queue

import pytest

from app.car_rent import (
    chain_combine_service_offers,
    chain_filter_offers,
    chain_book_car,
    PipelineContext,
    BOOKED_CARS,
    book_request,
)
from app.const import ERROR_TIMEOUT_MSG
from tests import AGGREGATOR_REQUEST, gen_offers, AGGREGATOR_RESPONSE, run_chain

pytestmark = pytest.mark.asyncio


class TestChainCombineService:
    """Тесты на корректную работу chain_combine_service_offers"""

    async def test_correct_output(self, event_loop, user_id):
        """Проверка ответа после отработки chain_combine_service_offers"""
        inbound = Queue()
        outbound = run_chain(chain_combine_service_offers, inbound)

        inbound.put_nowait(PipelineContext(user_id, AGGREGATOR_REQUEST))

        try:
            ctx = await asyncio.wait_for(outbound.get(), timeout=10.0)
            assert ctx.data == AGGREGATOR_RESPONSE
        except TimeoutError:
            raise TimeoutError(ERROR_TIMEOUT_MSG)

    async def test_time_execution(self, event_loop, user_id):
        """Проверка времени выполнения chain_combine_service_offers"""

        inbound = Queue()
        outbound = run_chain(chain_combine_service_offers, inbound)

        inbound.put_nowait(PipelineContext(user_id, AGGREGATOR_REQUEST))

        await asyncio.wait_for(outbound.get(), timeout=2.0)

    async def test_gathers_count(self, event_loop):
        """Проверка ограничения  на число параллельных вызовов get_offers_from_sourses"""

        inbound = Queue()
        outbound = run_chain(chain_combine_service_offers, inbound)

        chunks_count = 7
        for i in range(chunks_count):
            inbound.put_nowait(PipelineContext(i + 1, AGGREGATOR_REQUEST))

        timeout = 8.0
        start = time.time()

        res = list()
        for i in range(chunks_count):

            ctx = await outbound.get()
            res.append(ctx.data)

        assert res == [AGGREGATOR_RESPONSE for _ in range(chunks_count)]

        duration = time.time() - start
        assert timeout >= duration


class TestChainFilter:
    """Тесты звена chain_filter_offers"""

    @pytest.mark.parametrize(
        "brand, price, result",
        [
            (
                "LADA",
                None,
                [{
                    "url": f"http://source/car?id=1",
                    "price": 1_000,
                    "brand": "LADA"
                }],
            ),
            (
                "MITSUBISHI",
                None,
                [{
                    "url": f"http://source/car?id=2",
                    "price": 5_000,
                    "brand": "MITSUBISHI",
                }],
            ),
            (
                "KIA",
                None,
                [{
                    "url": f"http://source/car?id=3",
                    "price": 3_000,
                    "brand": "KIA"
                }],
            ),
            (
                "DAEWOO",
                None,
                [{
                    "url": f"http://source/car?id=4",
                    "price": 2_000,
                    "brand": "DAEWOO"
                }],
            ),
            (
                "PORSCHE",
                None,
                [{
                    "url": f"http://source/car?id=5",
                    "price": 10_000,
                    "brand": "PORSCHE",
                }],
            ),
            (
                None,
                1_000,
                [{
                    "url": f"http://source/car?id=1",
                    "price": 1_000,
                    "brand": "LADA"
                }],
            ),
            (
                None,
                3_000,
                [
                    {
                        "url": f"http://source/car?id=1",
                        "price": 1_000,
                        "brand": "LADA"
                    },
                    {
                        "url": f"http://source/car?id=3",
                        "price": 3_000,
                        "brand": "KIA"
                    },
                    {
                        "url": f"http://source/car?id=4",
                        "price": 2_000,
                        "brand": "DAEWOO",
                    },
                ],
            ),
            (
                "PORSCHE",
                10_000,
                [{
                    "url": f"http://source/car?id=5",
                    "price": 10_000,
                    "brand": "PORSCHE",
                }],
            ),
        ],
    )
    async def test_correct_filter(self, event_loop, user_id, brand, price,
                                  result):
        """Проверка различных кейсов фильтрации chain_filter_offers"""

        inbound = Queue()
        outbound = run_chain(chain_filter_offers,
                             inbound,
                             brand=brand,
                             price=price)

        inbound.put_nowait(PipelineContext(user_id, gen_offers("source")))

        try:
            ctx = await asyncio.wait_for(outbound.get(), timeout=10.0)
        except TimeoutError:
            raise TimeoutError(ERROR_TIMEOUT_MSG)

        assert ctx.data == result

    async def test_time_execution(self, event_loop, user_id):
        """Проверка бремени обработки chain_filter_offers"""

        inbound = Queue()
        outbound = run_chain(chain_filter_offers, inbound)

        inbound.put_nowait(PipelineContext(user_id, gen_offers("source")))

        await asyncio.wait_for(outbound.get(), timeout=0.1)


class TestBookRequest:
    """Тесты функции book_request"""

    async def test_book_request_cancel(self, event_loop, user_id):
        """Проверка обработки исполючения asyncio.CancelledError"""

        event = asyncio.Event()
        task: asyncio.Task = asyncio.create_task(
            book_request(
                user_id,
                {
                    "url": f"http://source/car?id=1",
                    "price": 1_000,
                    "brand": "LADA"
                },
                event,
            ))
        await asyncio.sleep(0.1)
        assert len(BOOKED_CARS[user_id]) == 1
        task.cancel()

        await task

        assert len(BOOKED_CARS[user_id]) == 0


class TestChainBooking:
    """Тесты звена chain_book_car"""

    async def test_time_execution(self, event_loop, user_id):
        """Тесты времени обработки chain_book_car"""

        inbound = Queue()
        outbound = run_chain(chain_book_car, inbound)

        inbound.put_nowait(PipelineContext(user_id, gen_offers("source")))

        await asyncio.wait_for(outbound.get(), timeout=2.5)

    async def test_1_booked_car(self, event_loop, user_id):
        """Проверка, что случилось только 1 бронирование, а остальные были отменены"""

        inbound = Queue()
        outbound = run_chain(chain_book_car, inbound)

        inbound.put_nowait(PipelineContext(user_id, gen_offers("source")))

        try:
            await asyncio.wait_for(outbound.get(), timeout=10.0)
        except TimeoutError:
            raise TimeoutError(ERROR_TIMEOUT_MSG)

        assert len(BOOKED_CARS.get(user_id)) == 1

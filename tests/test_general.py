import asyncio
from asyncio import Queue

import pytest

from app.car_rent import (
    PipelineContext,
    run_pipeline,
)
from app.const import ERROR_TIMEOUT_MSG
from tests import AGGREGATOR_REQUEST

pytestmark = pytest.mark.asyncio


class TestPipelineExecution:
    ITEMS_COUNT = 10

    async def _get_all_items(self, outbound: Queue) -> list[PipelineContext]:
        res = list()
        for _ in range(self.ITEMS_COUNT):
            res.append(await outbound.get())

        return res

    async def test_success(self, event_loop, user_id: int):
        inbound = Queue()
        outbound = run_pipeline(inbound)

        inbound.put_nowait(PipelineContext(user_id, AGGREGATOR_REQUEST))

        try:
            await asyncio.wait_for(outbound.get(), timeout=10.0)
        except TimeoutError:
            raise TimeoutError(ERROR_TIMEOUT_MSG)

    async def test_time_execution(self, event_loop):

        inbound = Queue()
        outbound = run_pipeline(inbound)

        for i in range(self.ITEMS_COUNT):
            inbound.put_nowait(PipelineContext(i + 1, AGGREGATOR_REQUEST))

        await asyncio.wait_for(self._get_all_items(outbound), timeout=22.0)

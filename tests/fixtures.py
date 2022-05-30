import pytest


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()


@pytest.fixture
def user_id() -> int:
    return 1


@pytest.fixture(autouse=True)
def clear_booked_cars():
    BOOKED_CARS.clear()

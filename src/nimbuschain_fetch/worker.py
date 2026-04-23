from __future__ import annotations

import asyncio
import signal

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.settings import get_settings


async def run_worker() -> None:
    settings = get_settings().model_copy(update={"nimbus_runtime_role": "worker"})
    fetcher = NimbusFetcher(settings=settings)
    await fetcher.start()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            # Some platforms do not support custom signal handlers in this context.
            pass

    try:
        await stop_event.wait()
    finally:
        await fetcher.stop()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()

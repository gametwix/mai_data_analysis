from os import environ
from asyncio import get_event_loop, ensure_future
from aiohttp.web import Application, AppRunner, TCPSite
from logging import getLogger, INFO
from redis.client import Redis
from controllers.redis_controller import RedisController

if __name__ == "__main__":

    logger = getLogger(environ['APP_NAME'])
    logger.setLevel(INFO)

    redis_client = Redis(host=environ['REDIS_HOST'], port=environ['REDIS_PORT'], decode_responses=True)

    controller = RedisController(redis_client)

    application = Application(logger=logger)

    async def main(loop=None):
        application.router.add_get("/redis", controller.get)
        application.router.add_put("/redis", controller.put)

        runner = AppRunner(application)

        await runner.setup()

        site = TCPSite(runner, environ['APP_HOST'], int(environ['APP_PORT']))

        await site.start()
    
    loop = get_event_loop()
    try:
        ensure_future(main(loop), loop=loop)
        loop.run_forever()
    except RuntimeError as exc:
        logger.exception(exc)
        raise(exc)
    finally:
        loop.run_until_complete(main(loop))
        loop.close()
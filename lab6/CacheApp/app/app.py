from os import environ
from asyncio import get_event_loop, ensure_future
from aiohttp.web import Application, AppRunner, TCPSite
from logging import getLogger, INFO
from kafka import KafkaProducer
from controllers.kafka_controller import KafkaController
import json
import time

if __name__ == "__main__":
    logger = getLogger(environ['APP_NAME'])
    logger.setLevel(INFO)
    #time.sleep(10)
    print([environ['KAFKA_HOST_1']+":"+environ['KAFKA_PORT_1'],
                                                    environ['KAFKA_HOST_2']+":"+environ['KAFKA_PORT_2']])
    kafka_client = KafkaProducer(bootstrap_servers=[environ['KAFKA_HOST_1']+":"+environ['KAFKA_PORT_1'],
                                                    environ['KAFKA_HOST_2']+":"+environ['KAFKA_PORT_2']], value_serializer=lambda m: json.dumps(m).encode('ascii'))

    controller = KafkaController(kafka_client, 'topic')

    application = Application(logger=logger)

    async def main(loop=None):
        application.router.add_put("/kafka", controller.put)

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
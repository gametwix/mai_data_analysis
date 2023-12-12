from aiohttp.web import Request, Response
from aiohttp_rest_api import AioHTTPRestEndpoint
from aiohttp_rest_api.responses import respond_with_json
from asyncio import get_event_loop, wait, shield
from json import loads, dumps

class KafkaController(AioHTTPRestEndpoint):
    def __init__(self, kafka_client = None, topic = None):
        if kafka_client is not None:
            self.kafka_client = kafka_client
        else:
            self.kafka_client = None
        if topic is not None:
            self.topic = topic
        else:
            self.topic = None
            

    async def put(self, request: Request) -> Response:
        data = None
        status = 500
        if request.query is not None and request.body_exists:
            body = await request.json()
            value_str = dumps(body)
            key = request.query['key']
            print(key, value_str)
            if self.kafka_client is not None:
                self.kafka_client.send(self.topic, {key : value_str})
                data = value_str
            else:
                value = True
                data = value_str
                
            if data:
                status = 201
            else:
                status = 404
        return respond_with_json(status=status, data=data)

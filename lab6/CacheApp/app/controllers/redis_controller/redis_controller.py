from aiohttp.web import Request, Response
from aiohttp_rest_api import AioHTTPRestEndpoint
from aiohttp_rest_api.responses import respond_with_json
from asyncio import get_event_loop, wait, shield
from redis.client import Redis
from json import loads, dumps

class RedisController(AioHTTPRestEndpoint):
    def __init__(self, redis_client: Redis=None):
        if redis_client is not None:
            self.redis_client = redis_client
        else:
            self.redis_client = None

    async def get(self, request: Request) -> Response:
        data = None
        status = 500
        if request.query is not None:
            key = request.query['key']
            if self.redis_client is not None:
                value = self.redis_client.get(key)
                data = loads(value)
            else:
                value = "OK"
                data = value
            if data is not None:
                status = 200
            else:
                status = 404
        return respond_with_json(status=status, data=data)

    async def put(self, request: Request) -> Response:
        data = None
        status = 500
        if request.query is not None and request.body_exists:
            body = await request.json()
            value_str = dumps(body)
            key = request.query['key']
            if self.redis_client is not None:
                value = self.redis_client.set(key, value_str)
                data = value
            else:
                value = True
                data = value_str
                
            if data:
                status = 201
            else:
                status = 404
        return respond_with_json(status=status, data=data)

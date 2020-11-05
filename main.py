"""
Created by Epic at 9/5/20
"""
from color_format import basicConfig

import speedcord
from speedcord.http import Route, HttpClient
from os import environ as env
from logging import getLogger, DEBUG
from aiohttp import ClientSession
from aiohttp.client_ws import ClientWebSocketResponse, WSMessage, WSMsgType
from ujson import loads

ws: ClientWebSocketResponse = None

client = speedcord.Client(intents=1)
basicConfig(getLogger())
logger = getLogger("worker")
logger.setLevel(DEBUG)

handlers = {}
total_guilds_served = 0


async def handle_worker():
    global ws
    session = ClientSession()
    async with session.ws_connect(f"ws://{env['WORKER_MANAGER_HOST']}:6060/workers") as ws:
        await ws.send_json({
            "t": "identify",
            "d": None
        })
        message: WSMessage
        async for message in ws:
            if message.type == WSMsgType.TEXT:
                data = message.json(loads=loads)
                handler = handlers.get(data["t"], None)
                if handler is None:
                    continue
                client.loop.create_task(handler(data["d"]))


async def handle_dispatch_bot_info(data: dict):
    client.token = data["token"]
    client.name = data["name"]

    logger.info(f"Started worker with name {client.name}!")
    client.http = HttpClient(client.token)
    await client.connect()


async def handle_request(data: dict):
    request_data = data["data"]
    method = request_data["method"]
    path = request_data["path"]
    params = request_data["route_params"]
    kwargs = params["kwargs"]

    route = Route(method, path, **params)
    logger.debug(f"{method} {path}")
    await client.http.request(route, **kwargs)


@client.listen("GUILD_CREATE")
async def on_guild_create(data, shard):
    global total_guilds_served
    await ws.send_json({"t": "add_guild", "d": data["id"]})
    total_guilds_served += 1
    logger.debug(f"New guild to serve: {data['name']}. Now serving {total_guilds_served} guilds.")


@client.listen("GUILD_DELETE")
async def on_guild_delete(data, shard):
    global total_guilds_served
    total_guilds_served -= 1
    await ws.send_json({"t": "remove_guild", "d": data["id"]})


handlers["request"] = handle_request
handlers["dispatch_bot_info"] = handle_dispatch_bot_info
client.loop.run_until_complete(handle_worker())
client.loop.run_forever()

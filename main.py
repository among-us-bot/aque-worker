"""
Created by Epic at 9/5/20
"""
from color_format import basicConfig

import speedcord
from speedcord.http import Route
from os import environ as env
from logging import getLogger, DEBUG
from aiohttp import ClientSession
from aiohttp.client_ws import ClientWebSocketResponse

ws: ClientWebSocketResponse = None

client = speedcord.Client(intents=512)
basicConfig(getLogger())
logger = getLogger("worker")
logger.setLevel(DEBUG)


async def handle_worker():
    global ws
    session = ClientSession()
    async with session.ws_connect(f"ws://{env['host']}:6060/worker") as ws:
        await ws.send_json({
            "t": "identify",
            "d": None
        })

client.loop.create_task(handle_worker())
client.token = env["TOKEN"]
client.run()

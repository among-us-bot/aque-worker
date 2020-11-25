"""
Created by Epic at 9/5/20
"""
from color_format import basicConfig

import speedcord
from speedcord.http import Route, HttpClient, LockManager
from os import environ as env
from logging import getLogger, DEBUG
from aiohttp import ClientSession
from aiohttp.client_ws import ClientWebSocketResponse, WSMessage, WSMsgType
from ujson import loads
from urllib.parse import quote as uriquote
from asyncio import Lock, sleep
from speedcord.exceptions import NotFound, Unauthorized, Forbidden, HTTPException,

ws: ClientWebSocketResponse = None

client = speedcord.Client(intents=1)
basicConfig(getLogger())
logger = getLogger("worker")
logger.setLevel(DEBUG)

handlers = {}
total_guilds_served = 0


class CustomHttp(HttpClient):
    async def request(self, route: Route, **kwargs):
        bucket = route.bucket

        for i in range(self.retry_attempts):
            if not self.global_lock.is_set():
                self.logger.debug("Sleeping for Global Rate Limit")
                await self.global_lock.wait()

            ratelimit_lock: Lock = self.ratelimit_locks.get(bucket, Lock(loop=self.loop))
            await ratelimit_lock.acquire()
            with LockManager(ratelimit_lock) as lockmanager:
                # Merge default headers with the users headers,
                # could probably use a if to check if is headers set?
                # Not sure which is optimal for speed
                kwargs["headers"] = {
                    **self.default_headers, **kwargs.get("headers", {})
                }

                # Format the reason
                try:
                    reason = kwargs.pop("reason")
                except KeyError:
                    pass
                else:
                    if reason:
                        kwargs["headers"]["X-Audit-Log-Reason"] = uriquote(
                            reason, safe="/ ")
                r = await self.session.request(route.method,
                                               self.baseuri + route.path,
                                               **kwargs)

                # check if we have rate limit header information
                remaining = r.headers.get('X-Ratelimit-Remaining')
                if remaining == '0' and r.status != 429:
                    # we've depleted our current bucket
                    delta = float(r.headers.get("X-Ratelimit-Reset-After"))
                    self.logger.debug(
                        f"Ratelimit exceeded. Bucket: {bucket}. Retry after: "
                        f"{delta}")
                    lockmanager.defer()
                    self.loop.call_later(delta, ratelimit_lock.release)

                status_code = r.status

                if status_code == 404:
                    raise NotFound(r)
                elif status_code == 401:
                    raise Unauthorized(r)
                elif status_code == 403:
                    raise Forbidden(r, await r.text())
                elif status_code == 429:
                    if not r.headers.get("Via"):
                        # Cloudflare banned?
                        raise HTTPException(r, await r.text())

                    data = await r.json()
                    retry_after = data["retry_after"] / 1000
                    is_global = data.get("global", False)
                    if is_global:
                        await ws.send_json({"t": "ratelimit", "d": "global"})
                        self.logger.warning(
                            f"Global ratelimit hit! Retrying in "
                            f"{retry_after}s")
                    else:
                        await ws.send_json({"t": "ratelimit", "d": bucket})
                        self.logger.warning(
                            f"A ratelimit was hit (429)! Bucket: {bucket}. "
                            f"Retrying in {retry_after}s")

                    await sleep(retry_after)
                    continue

                return r


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
    client.http = CustomHttp(client.token)
    await client.connect()


async def handle_request(data: dict):
    request_data = data["data"]
    method = request_data["method"]
    path = request_data["path"]
    params = request_data["route_params"]
    kwargs = params["kwargs"]

    route = Route(method, path, **params)
    logger.debug(f"{method} {path}")
    r = await client.http.request(route, **kwargs)
    if r.status < 200 or r.status >= 300:
        logger.warning(await r.text())


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

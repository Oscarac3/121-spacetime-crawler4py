import time
import cbor
import requests
from logging import Logger
from .config import Config
from .response import Response
from typing import Tuple

def download(url, config : Config, logger : Logger) -> Tuple[Response, float]:
    start = time.time()
    host, port = config.cache_server
    try:
        resp = requests.get(
            f"http://{host}:{port}/",
            params=[("q", f"{url}"), ("u", f"{config.user_agent}")])
    except Exception as e:
        logger.error(f"Download error {e} with url {url}. Continuing...")
        return Response({
            "error": f"Spacetime Request error {e} with url {url}.",
            "status": 500,
            "url": url}), 0
    try:
        if resp and resp.content:
            return Response(cbor.loads(resp.content)), time.time() - start
    except (EOFError, ValueError) as e:
        pass
    logger.error(f"Spacetime Response error {resp} with url {url}.")
    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": resp.status_code,
        "url": url}), 0

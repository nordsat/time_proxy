import asyncio
from datetime import datetime, timedelta
from fastapi import FastAPI, Response, Request
import httpx
from PIL import Image
from defusedxml.ElementTree import fromstring
from io import BytesIO
from cache import AsyncTTL

from fastapi.middleware.cors import CORSMiddleware

# Find the final URL to avoid the 302 redirect
SERVER = "https://wms-proxy-staging.int-nordmet-nordsat.s.ewcloud.host/viirs/"
CAP_STRING = "?REQUEST=GetCapabilities&SERVICE=WMS&VERSION=1.3.0"


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@AsyncTTL(time_to_live=60, skip_args=1)
async def get_capabilities(client: httpx.AsyncClient):
    return await client.get(SERVER + CAP_STRING)

async def get_timesteps(client: httpx.AsyncClient, requested_layer: str) -> list[datetime]:
    r_cap = await get_capabilities(client)

    if r_cap.history:
        print(f"Warning: Redirected to {r_cap.url}. Update your SERVER variable.")

    root = fromstring(r_cap.text)
    ns = {'wms': 'http://www.opengis.net/wms'}

    for layer in root.findall(".//wms:Layer", ns):
        if layer is None:
            continue

        name = layer.find("wms:Name", ns)
        if name is not None and name.text == requested_layer:
            dimension = layer.find("wms:Dimension", ns)
            if dimension is not None and dimension.text:
                times = dimension.text.split(",")
                parsed_times = [datetime.fromisoformat(t.strip()) for t in times]
                return parsed_times

    return []

async def fetch_image(client: httpx.AsyncClient, url: str, params: dict):
    """Worker function to fetch a single timestep asynchronously"""
    r = await client.get(url, params=params)
    if r.status_code == 200:
        return Image.open(BytesIO(r.content))
    return None

@app.get("/wms-proxy")
@app.get("/wms-proxy/")
async def wms_proxy(request: Request):
    request_params = dict(request.query_params)
    request_type = request_params.get("REQUEST", "").lower()

    async with httpx.AsyncClient(timeout=10.0) as client:

        if request_type != "getmap":
            r = await client.get(SERVER, params=request_params)
            return Response(r.content, status_code=r.status_code)

        requested_time_str = request_params.get("TIME", "")
        requested_layer = request_params.get("LAYERS", "")

        if not requested_layer or not requested_time_str:
            return Response("Missing LAYERS or TIME", status_code=400)

        # MapServer uses 'Z' for UTC, datetime handles it slightly differently depending on Python version.
        requested_time = datetime.fromisoformat(requested_time_str.replace('Z', '+00:00'))

        time_steps = await get_timesteps(client, requested_layer)

        fetch_tasks = []
        for t in time_steps:
            if requested_time - timedelta(hours=1) <= t <= requested_time:
                params = request_params.copy()
                params["TIME"] = t.isoformat()

                fetch_tasks.append(fetch_image(client, SERVER, params))

        downloaded_images = await asyncio.gather(*fetch_tasks)

    # Filter out any failed downloads (None values)
    valid_images = [img for img in downloaded_images if img is not None]

    if not valid_images:
        return Response("No valid data found for the requested time range.", status_code=404)

    base = valid_images[0].convert("RGBA")
    for img in valid_images[1:]:
        base = Image.alpha_composite(base, img.convert("RGBA"))

    img_byte_arr = BytesIO()
    base.save(img_byte_arr, format='PNG')
    return Response(content=img_byte_arr.getvalue(), media_type="image/png")

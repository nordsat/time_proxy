import asyncio
from datetime import datetime, timedelta
from fastapi import FastAPI, Response, Request
import httpx
from PIL import Image
from defusedxml.ElementTree import fromstring
import defusedxml.ElementTree as DET

from io import BytesIO
from cache import AsyncTTL
import os

import numpy as np
from numba import njit

import re
from fastapi.middleware.cors import CORSMiddleware
from hishel.httpx import AsyncCacheClient
import hishel

# Find the final URL to avoid the 302 redirect
SERVER = os.environ.get("WMS_SERVER", "http://localhost:8999")

CAP_STRING = "?REQUEST=GetCapabilities&SERVICE=WMS&VERSION=1.3.0"

cache_storage = hishel.AsyncSqliteStorage(default_ttl=3600)
policy = hishel.FilterPolicy()

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

@AsyncTTL(time_to_live=60, skip_args=1)
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
    r = await client.get(url, params=params)
    content_type = r.headers.get("Content-Type", "")

    if r.status_code == 200 and "image" in content_type:
        # skip empty images
        if len(r.content) < 15000:
            return None

        return r.content

    if "xml" in content_type:
        print(f"WMS Error: {r.text[:100]}")

    return None


@njit
def update_canvas(canvas, missing_mask, new_img):
    """Update the canvas only where missing_mask is True  and the new_img has valid data (alpha > 0)."""
    h, w, c = canvas.shape
    for y in range(h):
        for x in range(w):
            if missing_mask[y, x]:
                # If current image has data, fill the hole
                if new_img[y, x, 3] > 0:
                    for k in range(c):
                        canvas[y, x, k] = new_img[y, x, k]
                    # This pixel is no longer missing
                    missing_mask[y, x] = False
    return np.any(missing_mask) # Return True if we still have holes


def assemble_images_lazy(image_bytes_iterable: list[bytes]) -> bytes:
    """Process images one by one from latest to oldest."""
    if not image_bytes_iterable:
        return b""

    # We need to go newest -> oldest
    reversed_bytes = reversed(image_bytes_iterable)

    canvas = None
    missing_mask = None

    for b in reversed_bytes:
        # Load one image at a time
        try:
            img_array = np.asarray(Image.open(BytesIO(b)).convert("RGBA"))
        except Exception:
            # Print the first 200 characters of the non-image data
            print(f"MapServer sent this instead of an image: {b[:200]}")

        if canvas is None:
            # Initialize canvas with the latest image
            canvas = img_array.copy()
            # Initialize mask: True where alpha is 0
            missing_mask = (canvas[:, :, 3] == 0)
        else:
            # Update the canvas with the older image
            still_has_holes = update_canvas(canvas, missing_mask, img_array)

            # Efficiency: If the canvas is fully opaque, stop fetching/processing
            if not still_has_holes:
                break

    # Convert back to bytes
    final_img = Image.fromarray(canvas, "RGBA")
    img_byte_arr = BytesIO()
    final_img.save(img_byte_arr, format='PNG', optimize=False)
    return img_byte_arr.getvalue()


def parse_duration(duration_str: str) -> timedelta:
    """Translates strings like '30m', '2h', '1d' into a timedelta object."""
    match = re.match(r"^(\d+)([mhd])$", duration_str.lower())
    if not match:
        # Default fallback if the user types nonsense
        return timedelta(hours=1) 
    
    value, unit = int(match.group(1)), match.group(2)
    
    if unit == 'm':
        return timedelta(minutes=value)
    if unit == 'h':
        return timedelta(hours=value)
    if unit == 'd':
        return timedelta(days=value)
    
    return timedelta(hours=1)

@app.get("/{duration_str}")
@app.get("/{duration_str}/")
async def wms_proxy(duration_str: str, request: Request):
    request_params = dict(request.query_params)
    request_type = request_params.get("REQUEST", "").lower()

    async with httpx.AsyncClient(timeout=10.0) as client:

        if request_type != "getmap":
            r = await client.get(SERVER, params=request_params)
            server = b"https://wms-proxy-staging.int-nordmet-nordsat.s.ewcloud.host/viirs/"
            content = r.content.replace(server, server + duration_str.encode() + b"/")
            if request_type == "getcapabilities":
                content = simplify_capabilities_xml(content)
            return Response(content, status_code=r.status_code)

        requested_time_str = request_params.get("TIME", "")
        requested_layer = request_params.get("LAYERS", "")

        if not requested_layer or not requested_time_str:
            return Response("Missing LAYERS or TIME", status_code=400)

        # MapServer uses 'Z' for UTC, datetime handles it slightly differently depending on Python version.
        requested_time = datetime.fromisoformat(requested_time_str.replace('Z', '+00:00'))

        time_steps = await get_timesteps(client, requested_layer)

    async with AsyncCacheClient(storage=cache_storage, policy=policy, timeout=10.0) as client:
        duration = parse_duration(duration_str)
        fetch_tasks = []
        for t in time_steps:
            if requested_time - duration <= t <= requested_time:
                params = request_params.copy()
                params["TIME"] = t.isoformat()

                fetch_tasks.append(fetch_image(client, SERVER, params))

        downloaded_image_bytes = await asyncio.gather(*fetch_tasks)

    # Filter out any failed downloads (None values)
    valid_images_bytes = [img for img in downloaded_image_bytes if img is not None]

    if not valid_images_bytes:
        return Response("No valid data found for the requested time range.", status_code=404)

    final_image_payload = await asyncio.to_thread(assemble_images_lazy, valid_images_bytes)
    return Response(content=final_image_payload, media_type="image/png")

def simplify_capabilities_xml(xml_bytes: bytes, interval_min: int = 10) -> bytes:
    """
    Intersects the GetCapabilities XML to replace a comma-separated 
    list of times with a start/end/period interval.
    """
    # WMS 1.3.0 Namespace
    ns = {'wms': 'http://www.opengis.net/wms'}
    # Ensure namespaces don't get 'ns0' prefixes in the output
    DET.register_namespace('', ns['wms'])

    # Parse securely
    root = DET.fromstring(xml_bytes)

    for layer in root.findall(".//wms:Layer", ns):
        name_node = layer.find("wms:Name", ns)
        if name_node is not None:
            # Find the time dimension specifically
            dim = layer.find("./wms:Dimension[@name='time']", ns)
            if dim is not None and dim.text:
                # Extract the actual data points
                raw_times = [t.strip() for t in dim.text.split(",")]
                if len(raw_times) > 1:
                    # Determine the absolute range
                    start = raw_times[0]
                    end = raw_times[-1]
                    # Rewrite as an ISO 8601 Interval (Start/End/Period)
                    # This tells the client: "Stop asking for every minute."
                    dim.text = f"{start}/{end}/PT{interval_min}M"
    # Return as bytes for the Response object
    return DET.tostring(root, encoding='utf-8', xml_declaration=True)

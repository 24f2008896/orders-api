
import base64
import json
import time
import uuid
from collections import defaultdict, deque
from threading import Lock

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="Orders API")

# Assignment values
TOTAL_ORDERS = 56
RATE_LIMIT = 18
WINDOW_SECONDS = 10

# Allow the assignment grader's browser to call this API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)

# In-memory storage for this running application.
idempotency_store = {}
rate_limit_store = defaultdict(deque)
lock = Lock()


def encode_cursor(offset: int) -> str:
    """Turn a page offset into an opaque cursor."""
    raw = str(offset).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> int:
    """Convert a returned cursor back into a page offset."""
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        offset = int(raw.decode("utf-8"))
        if offset < 0 or offset > TOTAL_ORDERS:
            raise ValueError
        return offset
    except (ValueError, UnicodeDecodeError, Exception):
        raise HTTPException(status_code=400, detail="Invalid cursor")



@app.middleware("http")
async def enforce_rate_limit(request: Request, call_next):
    # CORS preflight requests do not consume the quota.
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id")

    # Only rate-limit requests that identify a client.
    if not client_id:
        return await call_next(request)

    now = time.monotonic()

    with lock:
        timestamps = rate_limit_store[client_id]

        while timestamps and now - timestamps[0] >= WINDOW_SECONDS:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT:
            retry_after = max(
                1,
                int(WINDOW_SECONDS - (now - timestamps[0]) + 0.999),
            )

            origin = request.headers.get("Origin", "*")

            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={
                    "Retry-After": str(retry_after),
                    "Access-Control-Allow-Origin": origin,
                    "Access-Control-Expose-Headers": "Retry-After",
                    "Vary": "Origin",
                },
            )


        timestamps.append(now)

    return await call_next(request)



@app.get("/")
def home():
    return {
        "message": "Orders API is running",
        "total_orders": TOTAL_ORDERS,
        "rate_limit": RATE_LIMIT,
        "window_seconds": WINDOW_SECONDS,
    }


@app.post("/orders", status_code=201)
def create_order(
    idempotency_key: str = Header(
        ..., alias="Idempotency-Key", min_length=1
    ),
):
    key = idempotency_key.strip()
    if not key:
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key cannot be empty",
        )

    with lock:
        # Reuse the original result for repeated keys.
        if key in idempotency_store:
            return JSONResponse(
                status_code=200,
                content=idempotency_store[key],
            )

        order = {
            "id": str(uuid.uuid4()),
            "status": "created",
        }
        idempotency_store[key] = order
        return JSONResponse(status_code=201, content=order)


@app.get("/orders")
def list_orders(
    limit: int = Query(default=10, ge=1, le=100),
    cursor: str | None = Query(default=None),
):
    offset = decode_cursor(cursor) if cursor else 0

    # The fixed catalog contains IDs 1 through 56.
    ids = range(offset + 1, min(offset + limit, TOTAL_ORDERS) + 1)
    items = [{"id": order_id} for order_id in ids]

    next_offset = offset + len(items)
    next_cursor = (
        encode_cursor(next_offset)
        if next_offset < TOTAL_ORDERS
        else None
    )

    return {
        "items": items,
        "next_cursor": next_cursor,
    }

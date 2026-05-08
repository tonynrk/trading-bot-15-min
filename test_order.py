"""Quick order placement test using existing bot credentials."""

import base64
import json
import uuid
from datetime import datetime, timezone

import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

BASE_URL   = "https://api.elections.kalshi.com/trade-api/v2"
KEY_FILE   = "reactions/apikey.json"
PK_FILE    = "reactions/privatekey.json"

# ── load credentials ─────────────────────────────────────────────────────────
with open(KEY_FILE) as f:
    API_KEY_ID = json.load(f)["code"].strip()

with open(PK_FILE) as f:
    pem = json.load(f)["code"].replace("\\n", "\n")
private_key = serialization.load_pem_private_key(
    pem.encode(), password=None, backend=default_backend()
)
print(f"API key : {API_KEY_ID[:12]}...")
print(f"Key size: {private_key.key_size} bits")

# ── signing ───────────────────────────────────────────────────────────────────
def sign(timestamp: str, method: str, path: str) -> str:
    msg = (timestamp + method + path).encode()
    sig = private_key.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=hashes.SHA256().digest_size),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode()

def make_headers(method: str, path: str) -> dict:
    ts = str(int(datetime.now(timezone.utc).timestamp() * 1000))
    return {
        "KALSHI-ACCESS-KEY":       API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": sign(ts, method, path),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type":            "application/json",
    }

sess = requests.Session()
sess.headers["User-Agent"] = "Mozilla/5.0"

# ── Step 1: find an open BTC 15m market ──────────────────────────────────────
print("\n── Step 1: find open KXBTC15M market ──")
r = sess.get(f"{BASE_URL}/markets?series_ticker=KXBTC15M&status=open&limit=1")
print(f"HTTP {r.status_code}")
markets = r.json().get("markets", [])
if not markets:
    print("No open market found — exiting")
    raise SystemExit

m = markets[0]
ticker = m["ticker"]
yes_ask = m.get("yes_ask_dollars") or m.get("yes_ask")
print(f"Ticker  : {ticker}")
print(f"YES ask : {yes_ask}")

# ── Step 2: check balance ─────────────────────────────────────────────────────
print("\n── Step 2: check balance ──")
path = "/trade-api/v2/portfolio/balance"
r = sess.get(f"https://api.elections.kalshi.com{path}", headers=make_headers("GET", path))
print(f"HTTP {r.status_code} | {r.text[:200]}")

# ── Step 3: place 1-cent limit buy (cheapest possible test) ──────────────────
print("\n── Step 3: place limit BUY yes_price=1 ──")
path = "/trade-api/v2/portfolio/orders"
order = {
    "ticker":           ticker,
    "action":           "buy",
    "side":             "yes",
    "count":            1,
    "type":             "limit",
    "yes_price":        1,
    "client_order_id":  str(uuid.uuid4()),
}
print(f"Payload : {json.dumps(order)}")
r = sess.post(
    f"https://api.elections.kalshi.com{path}",
    headers=make_headers("POST", path),
    json=order,
)
print(f"HTTP {r.status_code} | {r.text[:400]}")

if r.status_code == 201:
    data = r.json()
    oid = data.get("order", {}).get("order_id", "")
    print(f"\n✓ Order placed! order_id={oid}")

    # cancel it immediately (1-cent won't fill, but clean up)
    if oid:
        dpath = f"/trade-api/v2/portfolio/orders/{oid}"
        rd = sess.delete(
            f"https://api.elections.kalshi.com{dpath}",
            headers=make_headers("DELETE", dpath),
        )
        print(f"Cancel  : HTTP {rd.status_code}")
else:
    print(f"\n✗ Order failed")

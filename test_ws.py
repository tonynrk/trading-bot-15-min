import asyncio, ssl, certifi, websockets, base64, time, json
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

key = serialization.load_pem_private_key(
    open('reactions/privatekey.pem', 'rb').read(),
    password=None,
    backend=default_backend(),
)
key_id = json.load(open('reactions/apikey.json'))['code']

ts  = str(int(time.time() * 1000))
raw = ts + 'GET' + '/trade-api/ws/v2'
sig = base64.b64encode(
    key.sign(
        raw.encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256().digest_size),
        hashes.SHA256(),
    )
).decode()

hdrs = {
    'KALSHI-ACCESS-KEY':       key_id,
    'KALSHI-ACCESS-TIMESTAMP': ts,
    'KALSHI-ACCESS-SIGNATURE': sig,
}
ctx = ssl.create_default_context(cafile=certifi.where())

async def test():
    url = 'wss://api.elections.kalshi.com/trade-api/ws/v2'
    print(f'Connecting to {url} ...')
    async with websockets.connect(url, additional_headers=hdrs, ssl=ctx) as ws:
        print('CONNECTED!')
        sub = json.dumps({
            'id': 1,
            'cmd': 'subscribe',
            'params': {'channels': ['ticker'], 'market_tickers': ['KXBTC15M']},
        })
        await ws.send(sub)
        try:
            msg = await asyncio.wait_for(ws.recv(), timeout=8)
            print('MSG:', msg[:400])
        except asyncio.TimeoutError:
            print('No message in 8s (connected OK, no active market right now)')

asyncio.run(test())

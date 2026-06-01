import asyncio
# pyrefly: ignore [missing-import]
import websockets
import json

async def test_ws():
    async with websockets.connect("ws://127.0.0.1:8000/ws/torque") as ws:
        msg = await ws.recv()
        pkt = json.loads(msg)
        print("rf_scores:", pkt.get('rf_scores'))

asyncio.run(test_ws())

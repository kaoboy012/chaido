import asyncio  
from playwright.async_api import async_playwright  
async def test():  
    async with async_playwright() as p:  
        b = await p.chromium.connect_over_cdp('http://127.0.0.1:9222')  
        print('OK contexts:', len(b.contexts))  
asyncio.run(test())  

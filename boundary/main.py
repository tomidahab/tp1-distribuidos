# main.py
import asyncio
from Boundary import Boundary

async def main():
    server = Boundary(port=5000)
    await server.run()

if __name__ == "__main__":
    asyncio.run(main())

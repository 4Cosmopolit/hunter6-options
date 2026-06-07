import asyncio
import subprocess
import sys

async def run_service(module):
    proc = await asyncio.create_subprocess_exec(sys.executable, '-m', f'src.{module}')
    await proc.communicate()

async def main():
    services = ['detector', 'executor', 'hedger']
    tasks = [run_service(svc) for svc in services]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())

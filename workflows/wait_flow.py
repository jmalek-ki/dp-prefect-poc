from prefect import flow, task
from asyncio import sleep, run


@task
async def wait(seconds):
    print(f"Sleeping for {seconds} seconds.")
    await sleep(seconds)
    return None


@flow
async def subflow(seconds):
    await wait(seconds)
    return None


@flow
async def bigflow(seconds):
    await subflow(seconds)
    return None


# Convention, to make it easy to autodiscover by scripts:
entrypoint = bigflow

if __name__ == "__main__":
    run(bigflow(30))

from prefect import flow, task
from time import sleep


@task
def wait(seconds):
    print(f"Sleeping for {seconds} seconds.")
    sleep(seconds)
    return None


@flow
def subflow(seconds):
    wait(seconds)
    return None


@flow
def bigflow(seconds):
    subflow(seconds)
    return None


# Convention, to make it easy to autodiscover by scripts:
entrypoint = bigflow

if __name__ == "__main__":
    bigflow(30)

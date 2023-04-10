import datetime
import requests
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact


@task
def call_api(url):
    response = requests.get(url, verify=False)
    print(response.status_code)
    return response.json()


@task
def parse_fact(response):
    fact = response["fact"]
    print(fact)
    return fact


@task
def create_artifact(fact):
    now_time = str(datetime.datetime.utcnow())
    artifact_id = create_markdown_artifact(
        key=now_time.lower(),
        description=f"Your cat fact for {now_time}!",
        markdown=f"# Fact:\n\n{fact}"
    )
    return artifact_id


@flow
def api_workflow(url):
    fact_json = call_api(url)
    fact_text = parse_fact(fact_json)
    create_artifact(fact_text)
    return fact_text


# Convention, to make it easy to autodiscover by scripts:
entrypoint = api_workflow

if __name__ == '__main__':
    api_workflow("https://catfact.ninja/fact")

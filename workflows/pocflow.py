import requests
from prefect import flow, task


@task
def call_api(url):
    response = requests.get(url)
    print(response.status_code)
    return response.json()


@task
def parse_fact(response):
    fact = response["fact"]
    print(fact)
    return fact


@flow
def api_workflow(url):
    fact_json = call_api(url)
    fact_text = parse_fact(fact_json)
    return fact_text


# Convention, to make it easy to autodiscover by scripts:
entrypoint = api_workflow

if __name__ == '__main__':
    api_workflow("https://catfact.ninja/fact")

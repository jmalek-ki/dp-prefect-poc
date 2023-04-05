export POETRY_HTTP_BASIC_GOOGLE_USERNAME:=oauth2accesstoken
export POETRY_HTTP_BASIC_GOOGLE_PASSWORD:=$(shell gcloud auth print-access-token)

init:
	# setup a venv
	pyenv virtualenv dp-prefect-poc
	pyenv activate dp-prefect-poc
	# add repo to config
	poetry config repositories.google https://europe-python.pkg.dev/ki-artifact-registries-cf08/ki-pypi
	# install stuff
	poetry install --all-extras

format:
	poetry run importanize app && poetry run black app

ci-format:
	poetry run importanize app --ci && poetry run black app --check

ci-lint:
	poetry run flakehell lint app

build:
	docker build -t dp-prefect-poc -f docker/ki_int_dashboards.Dockerfile .

test:
	poetry run python -m pytest --headless tests

run-local-ui:
	poetry run prefect server start

run-local-agent:
	poetry run prefect agent start -q 'default'

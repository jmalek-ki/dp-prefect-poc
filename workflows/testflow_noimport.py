from prefect import flow


@flow
def test_stuff():
    print("I'm working")


entrypoint = test_stuff

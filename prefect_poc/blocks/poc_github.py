from prefect.filesystems import GitHub


def build_block():
    block = GitHub(
        repository="https://github.com/jmalek-ki/dp-prefect-poc.git",
    )
    block.get_directory("workflows") # specify a subfolder of repo
    block.save("poc-repo")


if __name__ == '__main__':
    build_block()

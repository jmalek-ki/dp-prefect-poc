from prefect.filesystems import GitHub
from prefect_poc import PROJECT_ROOT_DIR


def build_block():
    block = GitHub(
        repository="https://github.com/jmalek-ki/dp-prefect-poc",
        reference="workflows"
    )

    block.get_directory("workflows", PROJECT_ROOT_DIR)  # specify a subfolder of repo
    block.save("poc-repo", overwrite=True)


if __name__ == '__main__':
    build_block()

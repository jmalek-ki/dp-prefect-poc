import asyncio

from prefect.filesystems import GitHub
from prefect_poc import PROJECT_ROOT_DIR, WORKFLOW_DIR_SUFFIX


async def build_block(workflow_dir=PROJECT_ROOT_DIR):
    block = GitHub(
        repository="https://github.com/jmalek-ki/dp-prefect-poc",
        reference="workflows"  # branch name
    )

    await block.get_directory(WORKFLOW_DIR_SUFFIX, workflow_dir)  # specify a subfolder of repo
    await block.save("poc-repo", overwrite=True)
    return block


if __name__ == '__main__':
    asyncio.run(build_block())

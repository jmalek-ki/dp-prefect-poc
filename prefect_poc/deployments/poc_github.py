import asyncio
import glob
import importlib

from prefect.deployments import Deployment

from prefect_poc import PROJECT_ROOT_DIR, WORKFLOW_DIR, WORKFLOW_DIR_SUFFIX
from prefect_poc.blocks import poc_github


async def deploy_flow(flow):
    deployment = await Deployment.build_from_flow(flow)
    deployment_id = await deployment.apply()
    return deployment_id


async def deploy_all_flows():
    await poc_github.build_block(workflow_dir=PROJECT_ROOT_DIR)
    flow_files = glob.iglob("*.py", root_dir=WORKFLOW_DIR)
    built_deployments = set()

    for flow_file in flow_files:
        try:
            flow_mod = importlib.import_module(flow_file, WORKFLOW_DIR_SUFFIX)
        except ImportError as IErr:
            print(IErr)
            continue

        entrypoint = getattr(flow_mod, "entrypoint")
        if not entrypoint:
            continue

        deployment_id = await deploy_flow(entrypoint)
        built_deployments.add(deployment_id)

    return built_deployments


if __name__ == '__main__':
    asyncio.run(deploy_all_flows())

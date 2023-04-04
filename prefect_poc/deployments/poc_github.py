import asyncio
import glob
import importlib

from prefect.deployments import Deployment

from prefect_poc import PROJECT_ROOT_DIR, WORKFLOW_DIR, WORKFLOW_DIR_SUFFIX
from prefect_poc.blocks import poc_github


async def deploy_flow(flow, name):
    deployment = await Deployment.build_from_flow(flow, name)
    deployment_id = await deployment.apply()
    return deployment_id


async def deploy_all_flows():
    await poc_github.build_block(workflow_dir=PROJECT_ROOT_DIR)
    flow_files = glob.iglob("*.py", root_dir=WORKFLOW_DIR)
    built_deployments = dict()

    for flow_file in flow_files:
        if flow_file.startswith("__"):
            continue

        module_name = flow_file[:-3]
        import_name = f"{WORKFLOW_DIR_SUFFIX}.{module_name}"

        try:
            flow_mod = importlib.import_module(import_name)
        except ImportError as IErr:
            print(IErr)
            continue

        entrypoint = getattr(flow_mod, "entrypoint")
        if not entrypoint:
            continue

        deployment_name = getattr(flow_mod, "deployment_name", module_name)
        deployment_id = await deploy_flow(entrypoint, deployment_name)
        built_deployments[deployment_name] = deployment_id

    print(f"BUILT DEPLOYMENTS: {built_deployments}")
    return built_deployments


if __name__ == '__main__':
    asyncio.run(deploy_all_flows())

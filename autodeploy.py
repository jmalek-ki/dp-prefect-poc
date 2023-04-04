"""Autodeploys all flows from the 'workflows' branch of this repo
(provided the file has an 'entrypoint' variable pointed at a flow
we want it to deploy)
"""

import asyncio
from prefect_poc.deployments.poc_github import deploy_all_flows

if __name__ == '__main__':
    asyncio.run(deploy_all_flows())

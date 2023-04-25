import os

from prefect.utilities.asyncutils import sync_compatible

from prefect_gcp.credentials import GcpCredentials
from prefect_poc import PROJECT_ROOT_DIR, WORKFLOW_DIR_SUFFIX

GCP_CREDS_FILENAME = "gcpcreds.json"
GCP_CREDS_BLOCKNAME = "data-platform-gcp"


@sync_compatible
async def build_credentials_block(workflow_dir=PROJECT_ROOT_DIR):
    service_account_file = os.path.join(PROJECT_ROOT_DIR, GCP_CREDS_FILENAME)
    block = GcpCredentials(
        service_account_file=service_account_file
    )

    await block.get_directory(WORKFLOW_DIR_SUFFIX, workflow_dir)  # specify a subfolder of repo
    await block.save(GCP_CREDS_BLOCKNAME, overwrite=True)
    return block


if __name__ == '__main__':
    build_credentials_block()

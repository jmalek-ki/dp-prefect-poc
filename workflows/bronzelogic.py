import datetime
import typing

from prefect import flow, task
from prefect.utilities.asyncutils import sync_compatible
from prefect_gcp.cloud_storage import GcpCredentials, GcsBucket, cloud_storage_copy_blob


GCP_CREDS_BLOCK_NAME = "data-platform-gcp"

BlobName = str
GcsBucketName = str
VersioningStrategyFn = typing.Callable[[BlobName, GcsBucketName, GcsBucketName], BlobName]


def date_hrs_minutes_versioning(blob_name: BlobName, *args, **kwargs) -> BlobName:
    """Simple versioning strategy - puts each blob in a subfolder indicating
    the time of the copy process in the ISO datetime format with a precision
    of *minutes*, e.g. 'importantdata.json' => '2023-01-13T06:24'

    :param blob_name: Input filename. Required to compute the output.
    :return: Filename with the version prefix attached.
    """
    now = datetime.datetime.utcnow().isoformat(timespec="minutes")
    trg_blob_name = "/".join((now, blob_name))
    return trg_blob_name


def date_hour_versioning(blob_name: str, *args, **kwargs) -> str:
    """Simple versioning strategy - puts each blob in a subfolder indicating
    the time of the copy process in the ISO datetime format with a precision
    of *hours*, e.g. 'importantdata.json' => '2023-01-13T06'

    :param blob_name: Input filename. Required to compute the output.
    :return: Filename with the version prefix attached.
    """
    now = datetime.datetime.utcnow().isoformat(timespec="hours") + ":00"
    trg_blob_name = "/".join((now, blob_name))
    return trg_blob_name


# Specifies the default versioning strategy to use if not otherwise specified.
# NOTE: this can be set to None, in which case the output == input.
DEFAULT_VERSIONING_STRATEGY = date_hrs_minutes_versioning


@sync_compatible
async def _version_blob(
    blob_name: BlobName,
    src_bucket_name: GcsBucketName,
    trg_bucket_name: GcsBucketName = None,
    versioning_strategy: VersioningStrategyFn = None,
    gcp_creds: GcpCredentials = None,
    *version_args,
    **version_kwargs
) -> BlobName:
    """Copies the target BLOB (i.e. file) from the source bucket into the target bucket,
    adding version metadata to the path (i.e. will put the copies in subfolders, where
    the subfolder name corresponds to the file version). Does NOT copy the data locally
    in the process - the file data never leaves Google Cloud.

    :param blob_name: Filename to version.
    :param src_bucket_name: Bucket housing the file to version.
    :param trg_bucket_name: Optional; bucket to move data to. If not specified, assumed same as src_bucket_name.
    :param versioning_strategy: Optional function. Function used to generate the output filepaths.
                                If None, uses a function specified by DEFAULT_VERSIONING_STRATEGY.
                                Will receive blob and bucket names plus any generic args/kwargs.
    :param gcp_creds: Optional GcpCreds object. Created if not specified; mostly for optimization or overriding.
    :param version_args: Optional; positional args to pass to versioning_strategy
    :param version_kwargs: Optional; keyword args to pass to versioning_strategy

    :return: Filename of the *NEW COPY* on success.
    """
    _trg_bucket_name = trg_bucket_name or src_bucket_name
    _versioning_strategy = versioning_strategy or DEFAULT_VERSIONING_STRATEGY

    gcp_credentials = gcp_creds or GcpCredentials.load(GCP_CREDS_BLOCK_NAME)

    trg_blob_name = (
        _versioning_strategy(
            blob_name,
            src_bucket_name,
            trg_bucket_name,
            *version_args,
            **version_kwargs
        ) if _versioning_strategy
        else blob_name
    )

    copied_blob_name = await cloud_storage_copy_blob(
        source_bucket=src_bucket_name,
        source_blob=blob_name,
        dest_bucket=_trg_bucket_name,
        dest_blob=trg_blob_name,
        gcp_credentials=gcp_credentials
    )

    copied_blob_name = typing.cast(copied_blob_name, BlobName)
    return copied_blob_name


version_blob_task = task(

)(_version_blob)
version_blob_flow = flow(

)(_version_blob)


@flow()
@sync_compatible
async def version_all(
    src_bucket: GcsBucketName,
    src_folder: str = None,
    trg_bucket: GcsBucketName = None,
    versioning_strategy: VersioningStrategyFn = None,
    gcp_creds: GcpCredentials = None
):
    _src_folder = src_folder or ""
    gcp_credentials = gcp_creds or GcpCredentials.load(GCP_CREDS_BLOCK_NAME)

    bucket = GcsBucket(
        bucket=src_bucket,
        gcp_credentials=gcp_credentials
    )

    blobs_to_version = await bucket.list_blobs(folder=_src_folder)

    results = await version_blob_task.map(
        blobs_to_version,
        src_bucket_name=src_bucket,
        trg_bucket_name=trg_bucket,
        versioning_strategy=versioning_strategy,
        gcp_credentials=gcp_credentials
    )

    return results

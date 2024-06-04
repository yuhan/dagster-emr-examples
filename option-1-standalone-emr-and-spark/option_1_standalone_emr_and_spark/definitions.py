import shutil

from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
    file_relative_path,
)


@asset
def run_emr_job_in_subprocess(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:
    """
    https://docs.dagster.io/concepts/dagster-pipes/subprocess
    """
    cmd = [shutil.which("python"), file_relative_path(__file__, "./../../run_emr_job_modified.py")]
    return pipes_subprocess_client.run(
        command=cmd, context=context
    ).get_materialize_result()


defs = Definitions(
    assets=[run_emr_job_in_subprocess],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)

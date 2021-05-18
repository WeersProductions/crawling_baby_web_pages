from os.path import join
import subprocess
import sys
import os


def run_cmd(args_list, cwd=None):
    """
    Run a commandline argument and return its output+errors.

    Args:
        args_list (string[]): Argument list that is passed to the process.

    Raises:
        RuntimeError: [description]

    Returns:
        (Tuple): output, errors
    """
    print(args_list)
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
    (output, errors) = proc.communicate()
    if proc.returncode:
            raise RuntimeError('Error run_cmd: ' + errors)
    return (output, errors)


def run_spark_distributed(args_list):
    """ Runs a spark command on the cluster and checks whether the command succeeded.

    Args:
        args_list (string[]): Argument list that is passed to the process.
    """
    output, errors = run_cmd(args_list)
    lines = errors.splitlines()

    status_value = None
    for i in range(10):
        line = lines[-i].lstrip().rstrip()
        key_value_split = line.split(": ")
        if key_value_split[0] == "final status":
            status_value = key_value_split[1]
            break

    success = False
    if status_value is not None:
        success = status_value == "SUCCEEDED"

    return (success, output, errors)


def run_pipeline(script_location, project_base_path, user):
    """
    Runs a defined pipeline and mentions if any errors occur.

    Pipeline:
    1. spark-submit on cluster analysis/distribution_plot.py
    2. convert results to .pickle

    Returns:
        Tuple<boolean, string>: Success, reason
    """

    script_directory = os.path.dirname(os.path.abspath(script_location))
    print("Script directory", script_directory)

    # --- 1 ---
    # spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=20 --conf spark.yarn.maxAppAttempts=1 analysis/distribution_plot.py
    success, _, _ = run_spark_distributed(["spark-submit", "--master", "yarn", "--deploy-mode", "cluster", "--conf", "spark.dynamicAllocation.maxExecutors=20", "--conf", "spark.yarn.maxAppAttempts=1", os.path.join(script_directory, "distribution_plot.py")])
    if not success:
        return (False, "analysis/distribution_plot.py failed")

    # --- 2 ---
    # spark-submit <PROJECT_NAME>/util/parquet_file_to_pickle.py "A FILE PATH"
    _, _ = run_cmd(["spark-submit", os.path.join(project_base_path, "util/parquet_file_to_pickle.py"), os.path.join(project_base_path, "analysis/result/labelDistribution.parquet")], cwd=os.path.join("/home/", user))
    _, _ = run_cmd(["spark-submit", os.path.join(project_base_path, "util/parquet_file_to_pickle.py"), os.path.join(project_base_path, "analysis/result/fetchDistribution.parquet")], cwd=os.path.join("/home/", user))
    _, _ = run_cmd(["spark-submit", os.path.join(project_base_path, "util/parquet_file_to_pickle.py"), os.path.join(project_base_path, "analysis/result/diffExternalOutLinksDistribution.parquet")], cwd=os.path.join("/home/", user))
    _, _ = run_cmd(["spark-submit", os.path.join(project_base_path, "util/parquet_file_to_pickle.py"), os.path.join(project_base_path, "analysis/result/diffInternalOutLinksDistribution.parquet")], cwd=os.path.join("/home/", user))

    return (True, "Yay!")


def main(argv):
    """
    Run using: python <project_name>/analysis/analysis_pipeline.py
    In my case: python WebInsight/analysis/analysis_pipeline.py

    Note: This uses .parquet files that are created by loading/converter.py. Be sure to run that first. In case you want to, you can build a check at the start of this script to verify that people ran the converter :)

    Args:
        No args required. Script path is used to make sure resulting files are correctly placed in the file system from the caller's perspective.
    """
    PROJECT_BASE_PATH = "WebInsight"
    USER = "s1840495"


    print("Starting pipeline.")
    success, reason = run_pipeline(argv[0], PROJECT_BASE_PATH, USER)
    if not success:
        print("Failed to run:", reason)
    print("Finished pipeline.")


if __name__ == "__main__":
    main(sys.argv)

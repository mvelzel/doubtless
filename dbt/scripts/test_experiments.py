from dbt.cli.main import dbtRunner, dbtRunnerResult
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from multiprocessing import Process, Queue, set_start_method
from queue import Empty
import os
import argparse
import signal
import logging

logging.getLogger("thrift.transport").setLevel(logging.ERROR)

experiment_timeout = 15 * 60  # 15 minutes

operations = [
    "run_cross_join_overhead_experiment",
    "run_deduplication_overhead_experiment",
    "run_inner_join_overhead_experiment",
    "run_projection_overhead_experiment",
    "run_selection_overhead_experiment",
]

experiment_names = [
    "control",
    "small_bdds",
    "medium_bdds",
    "large_bdds",
    "medium_strings",
    "large_strings",
]


@contextmanager
def suppress_stdout_stderr():
    """A context manager that redirects stdout and stderr to devnull"""
    with open(os.devnull, "w") as fnull:
        with redirect_stderr(fnull) as err, redirect_stdout(fnull) as out:
            yield (err, out)


def run_experiment(dbt, experiment_name, target, operation, queue):
    logging.basicConfig(
        filename=f"experiments-{target}-{operation}.log",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
        filemode="a",
        force=True,
    )

    logger = logging.getLogger(__name__)

    with suppress_stdout_stderr():
        cli_args = [
            "run-operation", operation,
            "--args", "{ experiment_name: " + experiment_name + " }",
            "--target", target,
            "--quiet",
        ]
        logger.info(f"Invoking experiment {operation} {
                    experiment_name} through dbt.")
        res: dbtRunnerResult = dbt.invoke(cli_args)
        logger.info("Dbt command finished.")

    if res.success:
        res = res.result.results[0].execution_time
        if queue is not None:
            logger.info(f"Putting {operation} {
                        experiment_name} result {res} into queue.")
            queue.put(
                {"result": res}, timeout=5)
        else:
            logger.info(f"No queue found for {operation} {
                        experiment_name} result {res}.")

    else:
        logger.error(f"Running experiment {operation} {
                     experiment_name} failed.")
        logger.error(str(res))
        print(f"Running experiment {operation} {experiment_name} failed.")
        if queue is not None:
            queue.put({"result": None}, timeout=5)


def abort_running_experiments(dbt, target, operation):
    logging.basicConfig(
        filename=f"experiments-{target}-{operation}.log",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
        filemode="a",
        force=True,
    )

    logger = logging.getLogger(__name__)
    print("Aborting running experiments...")
    logger.info("Aborting running experiments...")

    with suppress_stdout_stderr():
        cli_args = [
            "run-operation", "abort_running_experiments",
            "--target", target,
            "--quiet",
        ]
        res: dbtRunnerResult = dbt.invoke(cli_args)

    if not res.success:
        print("Aborting running experiments failed.")
        print(res.exception)
        logger.error("Aborting running experiments failed.")
        logger.error(res.exception)
    else:
        print("Successfully aborted running experiments.")
        logger.info("Successfully aborted running experiments.")


def run_experiment_in_process(dbt, target, operation, experiment_name):
    logger = logging.getLogger(__name__)

    aborted = False
    exec_time = None

    queue = Queue()

    process = Process(target=run_experiment, args=(
        dbt,
        experiment_name,
        target,
        operation,
        queue
    ))
    process.start()

    process.join(experiment_timeout)

    if process.is_alive():
        logger.error(f"Experiment {operation} {
                     experiment_name} timed out after {
                     experiment_timeout / 60} minutes.")
        print(f"Experiment {operation} {experiment_name} timed out after {
              experiment_timeout / 60} minutes.")
        os.kill(process.pid, signal.SIGINT)

        if target == "postgres":
            abort_running_experiments(
                dbt,
                target
            )

        process.join(10)  # Give the process 10 seconds to abort.

        if process.is_alive():
            print("Forcing process termination...")
            logger.info("Forcing process termination...")
            process.terminate()

        aborted = True
    process.join()

    if aborted:
        exec_time = float("nan")
    else:
        try:
            queue_get = queue.get(timeout=5)
            exec_time = queue_get["result"]
        except Empty:
            exec_time = None

    print("Execution time: ", exec_time)
    queue = None


if __name__ == "__main__":
    set_start_method("spawn")

    signal.signal(signal.SIGINT, signal.default_int_handler)

    parser = argparse.ArgumentParser()

    # parser.add_argument(
    #     "--build-dataset",
    #     action=argparse.BooleanOptionalAction,
    #     help="Whether to build the dataset and its dependencies beforehand.",
    #     default=False
    # )
    parser.add_argument(
        "--target",
        help="Against what database to run the experiments.",
        choices=["spark", "postgres", "databricks"],
        default="spark"
    )
    parser.add_argument(
        "--operation",
        help="Which operation to run.",
        choices=operations,
    )
    parser.add_argument(
        "--experiment-name",
        help="Which experiment name to run.",
        choices=experiment_names,
    )

    args = parser.parse_args()

    logging.basicConfig(
        filename=f"experiments-{args.target}-{args.operation}.log",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
        filemode="w",
        force=True,
    )

    logger = logging.getLogger(__name__)
    logger.info("Started")

    dbt = dbtRunner()

    run_experiment_in_process(
        dbt, args.target, args.operation, args.experiment_name)

    logger.info("Finished")

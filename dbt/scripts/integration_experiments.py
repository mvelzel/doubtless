from dbt.cli.main import dbtRunner, dbtRunnerResult
import pickle
from tqdm import tqdm
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from multiprocessing import Process, Queue, set_start_method
from queue import Empty
import os
import argparse
import signal
import logging

logging.getLogger("thrift.transport").setLevel(logging.ERROR)

step_size = 30

experiment_timeout = 45 * 60  # 15 minutes

experiments = {
    "sentence_creation": {
        "operation": "run_name_price_matching_sentence_creation_experiment",
        "experiment_clusters_sizes": {
            1: range(1, 8434380+1, 8434380 // step_size + 1),
            2: range(1, 2024422+1, 2024422 // step_size + 1),
            3: range(1, 850446+1, 850446 // step_size + 1),
            4: range(1, 468156+1, 468156 // step_size + 1),
            5: range(1, 288195+1, 288195 // step_size + 1),
            6: range(1, 251544+1, 251544 // step_size + 1),
            7: range(1, 175714+1, 175714 // step_size + 1),
            8: range(1, 141016+1, 141016 // step_size + 1),
            9: range(1, 109665+1, 109665 // step_size + 1),
            10: range(1, 88940+1, 88940 // step_size + 1),
        },
        "datasets": ["int_english_offers_fields_transformed"],
    },
    "sentence_consolidation": {
        "operation": "run_name_price_matching_sentence_consolidation_experiment",
        "experiment_clusters_sizes": {
            1: range(1, 8434380+1, 8434380 // step_size + 1),
            2: range(1, 2024422+1, 2024422 // step_size + 1),
            3: range(1, 850446+1, 850446 // step_size + 1),
            4: range(1, 468156+1, 468156 // step_size + 1),
            5: range(1, 288195+1, 288195 // step_size + 1),
            6: range(1, 251544+1, 251544 // step_size + 1),
            7: range(1, 175714+1, 175714 // step_size + 1),
            8: range(1, 141016+1, 141016 // step_size + 1),
            9: range(1, 109665+1, 109665 // step_size + 1),
            10: range(1, 88940+1, 88940 // step_size + 1),
        },
        "datasets": ["int_english_offers_matching_sentence_enriched"],
    },
    "probability_dictionary_creation": {
        "operation": "run_name_price_matching_probability_dictionary_creation_experiment",
        "experiment_clusters_sizes": {
            1: range(1, 8434380+1, 8434380 // step_size + 1),
            2: range(1, 2024422+1, 2024422 // step_size + 1),
            3: range(1, 850446+1, 850446 // step_size + 1),
            4: range(1, 468156+1, 468156 // step_size + 1),
            5: range(1, 288195+1, 288195 // step_size + 1),
            6: range(1, 251544+1, 251544 // step_size + 1),
            7: range(1, 175714+1, 175714 // step_size + 1),
            8: range(1, 141016+1, 141016 // step_size + 1),
            9: range(1, 109665+1, 109665 // step_size + 1),
            10: range(1, 88940+1, 88940 // step_size + 1),
        },
        "datasets": ["int_english_offers_matching_sentence_enriched"],
    },
    "probability_calculation": {
        "operation": "run_name_price_matching_probability_calculation_experiment",
        "experiment_clusters_sizes": {
            1: range(1, 274648+1, 274648 // step_size + 1),
            2: range(1, 75262+1, 75262 // step_size + 1),
            3: range(1, 22380+1, 22380 // step_size + 1),
            4: range(1, 29064+1, 29064 // step_size + 1),
            5: range(1, 11070+1, 11070 // step_size + 1),
            6: range(1, 17214+1, 17214 // step_size + 1),
            7: range(1, 3759+1, 3759 // step_size + 1),
            8: range(1, 9304+1, 9304 // step_size + 1),
            9: range(1, 3519+1, 3519 // step_size + 1),
            10: range(1, 4380+1, 4380 // step_size + 1),
        },
        "datasets": [
            "english_offers_matching_dataset",
            "english_offers_matching_probability_dictionary",
        ],
    },
}


@contextmanager
def suppress_stdout_stderr():
    """A context manager that redirects stdout and stderr to devnull"""
    with open(os.devnull, "w") as fnull:
        with redirect_stderr(fnull) as err, redirect_stdout(fnull) as out:
            yield (err, out)


def run_experiment(
    dbt,
    cluster_size,
    data_size,
    target,
    operation,
    queue,
    all_experiments
):
    logging.basicConfig(
        filename=f"experiments-{target}-{all_experiments}.log",
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
            "--args", "{ cluster_size: " + str(cluster_size) +
            ", data_size: " + str(data_size) + " }",
            "--target", target,
            "--quiet",
        ]
        logger.info(f"Invoking experiment {
                    operation}, clusters: {
                    cluster_size}, size: {
                    data_size} through dbt.")
        res: dbtRunnerResult = dbt.invoke(cli_args)
        logger.info("Dbt command finished.")

    if res.success:
        res = res.result.results[0].execution_time
        if queue is not None:
            logger.info(f"Putting {operation}, clusters: {
                cluster_size}, size: {
                data_size} result {res} into queue.")
            queue.put(
                {"result": res}, timeout=5)
        else:
            logger.info(f"No queue found for {operation}, clusters: {
                cluster_size}, size: {
                data_size} result {res}.")

    else:
        logger.error(f"Running experiment {operation}, clusters: {
            cluster_size}, size: {
            data_size} failed.")
        logger.error(str(res))
        print(f"Running experiment {operation}, clusters: {
            cluster_size}, size: {
            data_size} failed.")
        if queue is not None:
            queue.put({"result": None}, timeout=5)


def abort_running_experiments(dbt, target, all_experiments):
    logging.basicConfig(
        filename=f"experiments-{target}-{all_experiments}.log",
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


def run_experiments_in_process(
    dbt,
    target,
    operation,
    cluster_size,
    data_sizes,
    all_experiments
):
    logger = logging.getLogger(__name__)

    execution_times = []

    aborted = False
    broken = False

    print(f"Running experiments over data sizes with clusters: {
          cluster_size}...")
    logger.info(f"Running experiments over data sizes with clusters: {
                cluster_size}...")

    for data_size in tqdm(data_sizes):
        exec_time = None

        if not aborted and not broken:
            queue = Queue()

            process = Process(target=run_experiment, args=(
                dbt,
                cluster_size,
                data_size,
                target,
                operation,
                queue,
                all_experiments
            ))
            process.start()

            process.join(experiment_timeout)

            if process.is_alive():
                logger.error(f"Experiment {operation} , clusters: {
                    cluster_size}, size: {
                    data_size} timed out after {
                    experiment_timeout / 60} minutes.")
                print(f"Experiment {operation} , clusters: {
                    cluster_size}, size: {
                    data_size} timed out after {
                    experiment_timeout / 60} minutes.")
                os.kill(process.pid, signal.SIGINT)

                if target == "postgres":
                    abort_running_experiments(
                        dbt,
                        target,
                        all_experiments
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
                    if exec_time is None:
                        broken = True
                except Empty:
                    exec_time = None
            queue = None
        elif aborted:
            exec_time = float("nan")
        elif broken:
            exec_time = None

        execution_times.append(exec_time)

    return execution_times


built_datasets = []


def build_dataset(dbt, target, dataset_selector):
    cli_args = ["build", "--select",
                dataset_selector, "--target", target, "--quiet"]
    dbt.invoke(cli_args)


def write_execution_times(execution_times, target, experiment):
    output_dir = f"experiment_results/{target}/{experiment}"

    if not os.path.isdir(f"experiment_results/{target}"):
        os.mkdir(f"experiment_results/{target}")
    if not os.path.isdir(f"experiment_results/{target}/{experiment}"):
        os.mkdir(f"experiment_results/{target}/{experiment}")

    with open(f"{output_dir}/execution_times.pkl", "wb") as file:
        pickle.dump(execution_times, file)


def run_integration_experiments(args, dbt, experiment, all_experiments):
    logger = logging.getLogger(__name__)

    experiment_settings = experiments[experiment]
    target = args.target

    print(f"Running {experiment} experiments for {target}.")
    logger.info(f"Running {experiment} experiments for {target}.")

    operation = experiment_settings["operation"]

    if args.build_dataset:
        global built_datasets

        datasets = experiment_settings["datasets"]
        unbuilt = [d for d in datasets if d not in built_datasets]
        dataset_selector = " ".join(unbuilt)

        print(f"Building datasets: {dataset_selector}")
        logger.info(f"Building datasets: {dataset_selector}")

        build_dataset(dbt, target, dataset_selector)
        built_datasets += unbuilt

    output_dir = f"experiment_results/{target}/{experiment}"

    if not args.rerun and os.path.isdir(output_dir):
        with open(f"{output_dir}/execution_times.pkl", "rb") as file:
            all_execution_times = pickle.load(file)
    else:
        all_execution_times = {}

        cluster_sizes = experiment_settings["experiment_clusters_sizes"].keys()

        for cluster_size in cluster_sizes:
            data_sizes = experiment_settings["experiment_clusters_sizes"][cluster_size]

            execution_times = run_experiments_in_process(
                dbt,
                target,
                operation,
                cluster_size,
                data_sizes,
                all_experiments
            )
            all_execution_times[cluster_size] = execution_times

    logger.info("Experiments for {experiment} finished.")
    print("Execution times:")
    print(all_execution_times)

    write_execution_times(all_execution_times, target, experiment)


if __name__ == "__main__":
    set_start_method("spawn")

    signal.signal(signal.SIGINT, signal.default_int_handler)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--build-dataset",
        action=argparse.BooleanOptionalAction,
        help="Whether to build the dataset and its dependencies beforehand.",
        default=False
    )
    parser.add_argument(
        "--rerun",
        action=argparse.BooleanOptionalAction,
        help="Whether to rerun the experiments if they already have been run.",
        default=False
    )
    parser.add_argument(
        "--target",
        help="Against what database to run the experiments.",
        choices=["spark", "postgres", "databricks"],
        default="spark"
    )
    parser.add_argument(
        "--experiments",
        nargs="*",
        help="Which integration experiments to run. Runs all by default.",
        choices=experiments.keys(),
        default=experiments.keys()
    )

    args = parser.parse_args()

    all_experiments = "-".join(args.experiments)

    logging.basicConfig(
        filename=f"experiments-{args.target}-{all_experiments}.log",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
        filemode="w",
        force=True,
    )

    logger = logging.getLogger(__name__)
    logger.info("Started")

    dbt = dbtRunner()

    # if args.build_dataset:
    #     print("Building core datasets...")
    #     logger.info("Building core datasets...")
    #     build_dataset(dbt, args.target,
    #                   "+int_sized_bdds_decoded +stg_dummy__gigabyte_dataset")

    for experiment in args.experiments:
        run_integration_experiments(args, dbt, experiment, all_experiments)

    logger.info("Finished")

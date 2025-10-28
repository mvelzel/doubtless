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

experiment_timeout = 15 * 60  # 15 minutes

operations = [
    "run_cross_join_overhead_experiment",
    "run_deduplication_overhead_experiment",
    "run_inner_join_overhead_experiment",
    "run_projection_overhead_experiment",
    "run_selection_overhead_experiment",
]

all_experiment_names = [
    "control",
    "small_bdds",
    "medium_bdds",
    "large_bdds",
    "medium_strings",
    "large_strings",
]

experiment_repeat_amount = 11

experiments = {
    "projection": {
        "operation": "run_projection_overhead_experiment",
        "experiment_names": {
            "spark": [
                "control",
                "small_bdds",
                "large_bdds",
                "large_strings"
            ],
            "databricks": [
                "control",
                "small_bdds",
                "large_bdds",
                "large_strings"
            ],
            "postgres": [
                "small_bdds",
                "large_bdds",
            ],
        },
        "datasets": {
            "control": ["selection_uncertainty_overhead_dataset__control"],
            "small_bdds": ["selection_uncertainty_overhead_dataset__small_bdds"],
            "large_bdds": ["selection_uncertainty_overhead_dataset__large_bdds"],
            "large_strings": ["selection_uncertainty_overhead_dataset__large_strings"]
        }
    },
    "selection": {
        "operation": "run_selection_overhead_experiment",
        "experiment_names": {
            "spark": [
                "control",
                "small_bdds",
                "large_bdds",
                "large_strings"
            ],
            "databricks": [
                "control",
                "small_bdds",
                "large_bdds",
                "large_strings"
            ],
            "postgres": [
                "small_bdds",
                "large_bdds",
            ],
        },
        "datasets": {
            "control": ["selection_uncertainty_overhead_dataset__control"],
            "small_bdds": ["selection_uncertainty_overhead_dataset__small_bdds"],
            "large_bdds": ["selection_uncertainty_overhead_dataset__large_bdds"],
            "large_strings": ["selection_uncertainty_overhead_dataset__large_strings"]
        }
    },
    "cross_join": {
        "operation": "run_cross_join_overhead_experiment",
        "experiment_names": {
            "spark": [
                "control",
                "small_bdds",
                "medium_bdds",
                "medium_strings"
            ],
            "databricks": [
                "control",
                "small_bdds",
                "medium_bdds",
                "medium_strings"
            ],
            "postgres": [
                "small_bdds",
                "medium_bdds",
            ],
        },
        "datasets": {
            "control": [
                "join_uncertainty_overhead_dataset__control__left",
                "join_uncertainty_overhead_dataset__control__right",
            ],
            "small_bdds": [
                "join_uncertainty_overhead_dataset__small_bdds__left",
                "join_uncertainty_overhead_dataset__small_bdds__right",
            ],
            "medium_bdds": [
                "join_uncertainty_overhead_dataset__medium_bdds__left",
                "join_uncertainty_overhead_dataset__medium_bdds__right",
            ],
            "medium_strings": [
                "join_uncertainty_overhead_dataset__medium_strings__left",
                "join_uncertainty_overhead_dataset__medium_strings__right",
            ],
        }
    },
    "inner_join": {
        "operation": "run_inner_join_overhead_experiment",
        "experiment_names": {
            "spark": [
                "control",
                "small_bdds",
                "medium_bdds",
                "medium_strings"
            ],
            "databricks": [
                "control",
                "small_bdds",
                "medium_bdds",
                "medium_strings"
            ],
            "postgres": [
                "small_bdds",
                "medium_bdds",
            ],
        },
        "datasets": {
            "control": [
                "join_uncertainty_overhead_dataset__control__left",
                "join_uncertainty_overhead_dataset__control__right",
            ],
            "small_bdds": [
                "join_uncertainty_overhead_dataset__small_bdds__left",
                "join_uncertainty_overhead_dataset__small_bdds__right",
            ],
            "medium_bdds": [
                "join_uncertainty_overhead_dataset__medium_bdds__left",
                "join_uncertainty_overhead_dataset__medium_bdds__right",
            ],
            "medium_strings": [
                "join_uncertainty_overhead_dataset__medium_strings__left",
                "join_uncertainty_overhead_dataset__medium_strings__right",
            ],
        }
    },
    "deduplication": {
        "operation": "run_deduplication_overhead_experiment",
        "experiment_names": {
            "spark": [
                "control",
                "small_bdds",
                "medium_bdds",
                "medium_strings"
            ],
            "databricks": [
                "control",
                "small_bdds",
                "medium_bdds",
                "medium_strings"
            ],
            "postgres": [
                "small_bdds",
                "medium_bdds",
            ],
        },
        "datasets": {
            "control": ["deduplication_uncertainty_overhead_dataset__control"],
            "small_bdds": ["deduplication_uncertainty_overhead_dataset__small_bdds"],
            "medium_bdds": ["deduplication_uncertainty_overhead_dataset__medium_bdds"],
            "medium_strings": ["deduplication_uncertainty_overhead_dataset__medium_strings"]
        }
    },
}


@contextmanager
def suppress_stdout_stderr():
    """A context manager that redirects stdout and stderr to devnull"""
    with open(os.devnull, "w") as fnull:
        with redirect_stderr(fnull) as err, redirect_stdout(fnull) as out:
            yield (err, out)


def run_experiment(dbt, experiment_name, target, operation, queue, all_experiments):
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


def run_experiments_in_process(dbt, target, operation, experiment_name, all_experiments, repeat_amount):
    logger = logging.getLogger(__name__)

    execution_times = []

    aborted = False
    broken = False

    print(f"Running {experiment_name}...")
    logger.info(f"Running {experiment_name}...")

    for i in tqdm(range(repeat_amount)):
        exec_time = None

        if not aborted and not broken:
            queue = Queue()

            process = Process(target=run_experiment, args=(
                dbt,
                experiment_name,
                target,
                operation,
                queue,
                all_experiments
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


def run_overhead_experiments(args, dbt, experiment, all_experiments):
    logger = logging.getLogger(__name__)

    experiment_settings = experiments[experiment]
    target = args.target

    print(f"Running {experiment} experiments for {target}.")
    logger.info(f"Running {experiment} experiments for {target}.")

    operation = experiment_settings["operation"]
    experiment_names = experiment_settings["experiment_names"][target]
    select_experiment_names = args.select_experiment_names
    experiment_names = [
        name for name in experiment_names if name in select_experiment_names]

    if args.build_dataset:
        global built_datasets

        datasets = [
            d for name, ds in experiment_settings["datasets"].items()
            if name in experiment_names
            for d in ds
        ]
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

        for experiment_name in experiment_names:
            execution_times = run_experiments_in_process(
                dbt, target, operation, experiment_name, all_experiments, args.repeat_amount)
            all_execution_times[experiment_name] = execution_times

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
        "--repeat-amount",
        type=int,
        help="How often to repeat each experiment.",
        default=experiment_repeat_amount
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
        help="Which overhead experiments to run. Runs all by default.",
        choices=experiments.keys(),
        default=experiments.keys()
    )
    parser.add_argument(
        "--select-experiment-names",
        nargs="*",
        help="Which individual experiment names to run. Runs all by default.",
        choices=all_experiment_names,
        default=all_experiment_names,
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

    if args.build_dataset:
        build_dataset(dbt, args.target,
                      "+stg_dummy__sized_bdds +stg_dummy__gigabyte_dataset")

    for experiment in args.experiments:
        run_overhead_experiments(args, dbt, experiment, all_experiments)

    logger.info("Finished")

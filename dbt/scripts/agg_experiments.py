from dbt.cli.main import dbtRunner, dbtRunnerResult
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from multiprocessing import Process, Queue, set_start_method
from queue import Empty
import matplotlib.ticker as mticker
import os
import hashlib
import numpy as np
import yaml
import matplotlib.pyplot as plt
import json
import argparse
from tqdm import tqdm
from sklearn.model_selection import ParameterGrid
import signal
import logging

logging.getLogger("thrift.transport").setLevel(logging.ERROR)

param_grid = {
    "prune_method": ["none", "each-operation", "each-step", "on-finish"]
}

experiment_timeout = 15 * 60  # 15 minutes

experiments = {
    "count": {
        "dataset": "probabilistic_count_dataset",
        "experiment_names": [
            [
                f"prob_count_{variables}_{alternatives}"
                # for alternatives in range(1, 13)
                # for alternatives in range(1, 3)
                for alternatives in range(1, 15)
            ]
            # for variables in range(1, 13)
            # for variables in range(1, 3)
            for variables in range(1, 15)
        ],
        "operation": "run_prob_count_experiment"
    },
    "sum": {
        "dataset": "probabilistic_sum_dataset",
        "experiment_names": [
            [
                f"prob_sum_{variables}_{alternatives}"
                # for alternatives in range(1, 7)
                # for alternatives in range(1, 3)
                for alternatives in range(1, 10)
            ]
            # for variables in range(1, 7)
            # for variables in range(1, 3)
            for variables in range(1, 10)
        ],
        "operation": "run_prob_sum_experiment"
    },
    "min": {
        "dataset": "probabilistic_min_dataset",
        "experiment_names": [
            [
                f"prob_min_{variables}_{alternatives}"
                # for alternatives in range(1, 26)
                # for alternatives in range(1, 3)
                for alternatives in range(1, 30)
            ]
            # for variables in range(1, 26)
            # for variables in range(1, 3)
            for variables in range(1, 30)
        ],
        "operation": "run_prob_min_experiment"
    },
    "max": {
        "dataset": "probabilistic_max_dataset",
        "experiment_names": [
            [
                f"prob_max_{variables}_{alternatives}"
                # for alternatives in range(1, 26)
                # for alternatives in range(1, 3)
                for alternatives in range(1, 30)
            ]
            # for variables in range(1, 26)
            # for variables in range(1, 3)
            for variables in range(1, 30)
        ],
        "operation": "run_prob_max_experiment"
    },
    "avg": {
        "dataset": "probabilistic_avg_dataset",
        "experiment_names": [
            [
                f"prob_avg_{variables}_{alternatives}"
                # for alternatives in range(1, 6)
                # for alternatives in range(1, 3)
                for alternatives in range(1, 10)
            ]
            # for variables in range(1, 6)
            # for variables in range(1, 3)
            for variables in range(1, 10)
        ],
        "operation": "run_prob_avg_experiment"
    },
}


def build_dataset(dbt, target, dataset):
    cli_args = ["build", "--select",
                f"+{dataset}", "--target", target, "--quiet"]
    dbt.invoke(cli_args)


@contextmanager
def suppress_stdout_stderr():
    """A context manager that redirects stdout and stderr to devnull"""
    with open(os.devnull, "w") as fnull:
        with redirect_stderr(fnull) as err, redirect_stdout(fnull) as out:
            yield (err, out)


def run_experiment(dbt, experiment_name, target, operation, config, queue):
    logging.basicConfig(
        filename=f"experiments-{target}.log",
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
            "--vars", yaml.dump(config),
        ]
        logger.info(f"Invoking experiment {experiment_name} through dbt.")
        res: dbtRunnerResult = dbt.invoke(cli_args)
        logger.info("Dbt command finished.")

    if res.success:
        res = res.result.results[0].execution_time
        if queue is not None:
            logger.info(f"Putting {experiment_name} result {res} into queue.")
            queue.put(
                {"result": res}, timeout=5)
        else:
            logger.info(f"No queue found for {experiment_name} result {res}.")

    else:
        logger.error(f"Running experiment {experiment_name} failed.")
        logger.error(str(res))
        print(f"Running experiment {experiment_name} failed.")
        if queue is not None:
            queue.put({"result": None}, timeout=5)


def abort_running_experiments(dbt, target):
    logging.basicConfig(
        filename=f"experiments-{target}.log",
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


def write_execution_times(execution_times, target, agg_name, hash):
    conf_dir = f"experiment_results/{target}/{agg_name}/{hash}"

    if not os.path.isdir(f"experiment_results/{target}"):
        os.mkdir(f"experiment_results/{target}")
    if not os.path.isdir(f"experiment_results/{target}/{agg_name}"):
        os.mkdir(f"experiment_results/{target}/{agg_name}")
    if not os.path.isdir(
        f"experiment_results/{target}/{agg_name}/{hash}"
    ):
        os.mkdir(f"experiment_results/{target}/{agg_name}/{hash}")

    with open(f"{conf_dir}/config.json", "w") as f:
        json.dump(config, f)
    np.save(f"{conf_dir}/execution_times.npy", execution_times)


def run_all_experiments(dbt, target, agg_name, config, test_run=False):
    logger = logging.getLogger(__name__)

    hash = hashlib.md5(json.dumps(
        config, sort_keys=True).encode("utf-8")
    ).hexdigest()

    experiment_names = experiments[agg_name]["experiment_names"]
    if test_run:
        experiment_names = [experiment_names[0]]

    operation = experiments[agg_name]["operation"]

    execution_times: list[float] = []

    # Warmup the database so the first experiment is not too long
    run_experiment(
        dbt,
        experiment_names[0][0],
        target,
        operation,
        config,
        None
    )

    for i, variables in enumerate(tqdm(experiment_names)):
        if test_run:
            variables = [variables[0]]

        execution_times.append([])
        aborted = False
        broken = False
        for name in tqdm(variables):
            exec_time = None

            if not aborted and not broken:
                queue = Queue()

                process = Process(target=run_experiment, args=(
                    dbt,
                    name,
                    target,
                    operation,
                    config,
                    queue
                ))
                process.start()

                process.join(experiment_timeout)

                if process.is_alive():
                    logger.error(f"Experiment {name} timed out after {
                        experiment_timeout / 60} minutes.")
                    print(f"Experiment {name} timed out after {
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
                        if exec_time is None:
                            broken = True
                    except Empty:
                        exec_time = None
                queue = None
            elif aborted:
                exec_time = float("nan")
            elif broken:
                exec_time = None

            execution_times[i].append(exec_time)

        if not test_run:
            write_execution_times(execution_times, target, agg_name, hash)

    return execution_times


def log_tick_formatter(val, pos=None):
    return f"$10^{{{int(val)}}}$"
    # return f"{10**val:.2e}"      # e-Notation


built_datasets = []


def run_aggregation_experiments(args, dbt, agg_name, config):
    logger = logging.getLogger(__name__)

    agg_experiment_settings = experiments[agg_name]

    print(f"Running experiments for {
          agg_name} aggregation with config: {config}.")
    logger.info(f"Running experiments for {
        agg_name} aggregation with config: {config}.")

    target = args.target

    if args.build_dataset \
            and agg_experiment_settings["dataset"] not in built_datasets:
        print("Building dataset...")
        build_dataset(dbt, target, agg_experiment_settings["dataset"])
        built_datasets.append(agg_experiment_settings["dataset"])

    hash = hashlib.md5(json.dumps(
        config, sort_keys=True).encode("utf-8")
    ).hexdigest()

    conf_dir = f"experiment_results/{target}/{agg_name}/{hash}"

    test_run = args.test_run

    if not args.rerun and os.path.isdir(conf_dir):
        execution_times = np.load(f"{conf_dir}/execution_times.npy")
    else:
        execution_times = np.array(
            run_all_experiments(dbt, target, agg_name, config, test_run)
        )

        if not test_run:
            write_execution_times(execution_times, target, agg_name, hash)

    logger.info("Experiments for {agg_name} finished.")
    print("Execution times:")
    print(execution_times)

    try:
        xrange = np.arange(1, execution_times.shape[0] + 1)
        yrange = np.arange(1, execution_times.shape[1] + 1)

        X, Y = np.meshgrid(xrange, yrange)

        fig = plt.figure()
        plt.title(f"{agg_name}\n{config['prune_method']}")
        ax = fig.add_subplot(111, projection='3d')
        ax.plot_surface(X, Y, np.log10(execution_times), cmap="viridis")

        ax.set_xlabel("Variables")
        ax.set_ylabel("Alternatives per Variable")
        ax.set_zlabel("Execution Time ($\\log^{10}$)")

        ax.zaxis.set_major_formatter(mticker.FuncFormatter(log_tick_formatter))
        ax.zaxis.set_major_locator(mticker.MaxNLocator(integer=True))

        ax.set_xticks(xrange, [f"{x}" for x in xrange])
        ax.set_yticks(yrange, [f"{y}" for y in yrange])

        if not test_run:
            plt.savefig(f"{conf_dir}/3dplot.png")

        world_counts = (Y ** X).flatten()
        flat_execution_times = execution_times.flatten()

        plt.figure()
        plt.title(f"{agg_name}\n{config['prune_method']}")
        plt.xscale("log")
        plt.yscale("log")
        plt.scatter(world_counts, flat_execution_times)

        ymin, ymax = plt.ylim()

        plt.vlines(
            x=world_counts[np.isnan(flat_execution_times)],
            ymin=ymin,
            ymax=ymax,
            color='red',
            linestyle='--',
            linewidth=1,
            label='NaN'
        )

        if not test_run:
            plt.savefig(f"{conf_dir}/2dplot.png")
    except Exception:
        pass


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
        help="Whether to rerun the experiments for a given config.",
        default=False
    )
    parser.add_argument(
        "--target",
        help="Against what database to run the experiments.",
        choices=["spark", "postgres", "databricks"],
        default="spark"
    )
    parser.add_argument(
        "--test-run",
        action=argparse.BooleanOptionalAction,
        help="Whether to do a small test run instead of the full experiments.",
        default=False
    )
    parser.add_argument(
        "--all-parameters",
        action=argparse.BooleanOptionalAction,
        help="Whether to run experiments on the permutation of all params.",
        default=True
    )
    parser.add_argument(
        "--experiments",
        nargs="*",
        help="Which aggregation experiments to run.",
        choices=experiments.keys(),
        default=[]
    )
    parser.add_argument(
        "--show-plots",
        action=argparse.BooleanOptionalAction,
        help="Whether to show the final experiment result plots.",
        default=False
    )

    args = parser.parse_args()

    logging.basicConfig(
        filename=f"experiments-{args.target}.log",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
        filemode="w",
        force=True,
    )

    logger = logging.getLogger(__name__)
    logger.info("Started")

    dbt = dbtRunner()

    params = list(ParameterGrid(param_grid))

    if len(args.experiments) == 0:
        experiment_names = experiments.keys()
    else:
        experiment_names = args.experiments

    if args.all_parameters:
        for config in params:
            for agg_name in experiment_names:
                run_aggregation_experiments(args, dbt, agg_name, config)

                if not args.show_plots:
                    plt.close("all")
    else:
        for agg_name in experiment_names:
            run_aggregation_experiments(args, dbt, agg_name, params[0])

            if not args.show_plots:
                plt.close("all")

    if args.show_plots:
        plt.show()

    logger.info("Finished")

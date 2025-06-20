from dbt.cli.main import dbtRunner, dbtRunnerResult
import matplotlib.ticker as mticker
import os
import hashlib
import pathlib
import numpy as np
import matplotlib.pyplot as plt
import json
import contextlib
import yaml
import argparse
from tqdm import tqdm
from sklearn.model_selection import ParameterGrid
import signal

param_grid = {
    "prune-method": ["none", "each-operation", "each-step", "on-finish"]
}

experiments = {
    "count": {
        "dataset": "probabilistic_count_dataset",
        "experiment_names": [
            [
                f"prob_count_{variables}_{alternatives}"
                for alternatives in range(1, 13)
            ]
            for variables in range(1, 13)
        ],
        "operation": "run_prob_count_experiment"
    },
    "sum": {
        "dataset": "probabilistic_sum_dataset",
        "experiment_names": [
            [
                f"prob_sum_{variables}_{alternatives}"
                for alternatives in range(1, 7)
            ]
            for variables in range(1, 7)
        ],
        "operation": "run_prob_sum_experiment"
    },
    "min": {
        "dataset": "probabilistic_min_dataset",
        "experiment_names": [
            [
                f"prob_min_{variables}_{alternatives}"
                for alternatives in range(1, 26)
            ]
            for variables in range(1, 26)
        ],
        "operation": "run_prob_min_experiment"
    },
    "max": {
        "dataset": "probabilistic_max_dataset",
        "experiment_names": [
            [
                f"prob_max_{variables}_{alternatives}"
                for alternatives in range(1, 26)
            ]
            for variables in range(1, 26)
        ],
        "operation": "run_prob_max_experiment"
    },
    "avg": {
        "dataset": "probabilistic_avg_dataset",
        "experiment_names": [
            [
                f"prob_avg_{variables}_{alternatives}"
                for alternatives in range(1, 6)
            ]
            for variables in range(1, 6)
        ],
        "operation": "run_prob_avg_experiment"
    },
}


def build_dataset(dbt, target, dataset):
    cli_args = ["build", "--select",
                f"+{dataset}", "--target", target, "--quiet"]
    dbt.invoke(cli_args)


def run_experiment(dbt, experiment_name, target, operation):
    cli_args = [
        "run-operation", operation,
        "--args", "{ experiment_name: " + experiment_name + " }",
        "--target", target,
        "--quiet"
    ]
    res: dbtRunnerResult = dbt.invoke(cli_args)

    if res.success:
        return res.result.results[0].execution_time
    else:
        if res.exception is not None:
            raise res.exception
        else:
            raise Exception(f"Running experiment {experiment_name} failed.")


def run_all_experiments(dbt, target, agg_name, test_run=False):
    experiment_names = experiments[agg_name]["experiment_names"]
    if test_run:
        experiment_names = [experiment_names[0]]

    operation = experiments[agg_name]["operation"]

    # Warmup the database so the first experiment is not disproportionally long
    run_experiment(dbt, experiment_names[0][0], target, operation)

    execution_times: list[float] = []
    for i, variables in enumerate(tqdm(experiment_names)):
        if test_run:
            variables = [variables[0]]

        execution_times.append([])
        for name in tqdm(variables):
            time = float("nan")
            try:
                time = float(run_experiment(
                    dbt, name, target, operation
                ))
            except Exception as e:
                print(e)
            execution_times[i].append(time)

    return execution_times


def log_tick_formatter(val, pos=None):
    return f"$10^{{{int(val)}}}$"
    # return f"{10**val:.2e}"      # e-Notation


def write_config(target, config_path, config):
    if target == "spark":
        new_config = {
            "com": {
                "doubtless": {
                    "spark": config
                }
            }
        }

        with open(config_path, "w") as config_file:
            json.dump(new_config, config_file, indent=4, sort_keys=True)
    elif target == "postgres":
        new_config = {
            "config": config
        }

        cli_args = [
            "run-operation", "set_config",
            "--args", yaml.dump(new_config),
            "--target", target,
            "--quiet"
        ]
        dbt.invoke(cli_args)


def run_aggregation_experiments(args, dbt, agg_name):
    agg_experiment_settings = experiments[agg_name]

    print(f"Running experiments for {agg_name} aggregation.")

    target = args.target

    if args.build_dataset:
        print("Building dataset...")
        build_dataset(dbt, target, agg_experiment_settings["dataset"])

    if target == "spark":
        with contextlib.redirect_stdout(None):
            cli_args = [
                "show", "--inline", "select config()",
                "--target", target,
                "--quiet"
            ]
            config_res: dbtRunnerResult = dbt.invoke(cli_args)

        config = json.loads(
            config_res.result.results[0].agate_table.columns[0].values()[0]
        )
    elif target == "postgres":
        with contextlib.redirect_stdout(None):
            cli_args = [
                "show",
                "--inline", "select * from experiments.experiments_config",
                "--target", target,
                "--quiet"
            ]
            config_res: dbtRunnerResult = dbt.invoke(cli_args)

        config = json.loads(
            config_res.result.results[0].agate_table.columns[0].values()[0]
        )

    print("Current config:")
    print(json.dumps(config, sort_keys=True, indent=4))

    hash = hashlib.md5(json.dumps(
        config, sort_keys=True).encode("utf-8")
    ).hexdigest()

    conf_dir = f"experiment_results/{target}/{agg_name}/{hash}"

    test_run = args.test_run

    if not args.rerun and os.path.isdir(conf_dir):
        execution_times = np.load(f"{conf_dir}/execution_times.npy")
    else:
        execution_times = np.array(
            run_all_experiments(dbt, target, agg_name, test_run)
        )

        if not test_run:
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

    print("Execution times:")
    print(execution_times)

    xrange = np.arange(1, execution_times.shape[0] + 1)
    yrange = np.arange(1, execution_times.shape[1] + 1)

    X, Y = np.meshgrid(xrange, yrange)

    fig = plt.figure()
    plt.title(f"{agg_name}\n{config['aggregations']['prune-method']}")
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
    plt.title(f"{agg_name}\n{config['aggregations']['prune-method']}")
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


if __name__ == "__main__":
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
        choices=["spark", "postgres"],
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

    default_config_path = pathlib.Path(__file__).parent.parent.parent
    default_config_path = default_config_path / "spark" / \
        "src" / "main" / "resources" / "application.conf"
    default_config_path = default_config_path.resolve()

    parser.add_argument(
        "--config",
        help="The location of the application.conf file",
        default=default_config_path)

    args = parser.parse_args()

    dbt = dbtRunner()

    params = list(ParameterGrid(param_grid))

    if len(args.experiments) == 0:
        experiment_names = experiments.keys()
    else:
        experiment_names = args.experiments

    if args.all_parameters:
        for config_values in params:
            config = {
                "aggregations": config_values
            }
            write_config(args.target, args.config, config)

            for agg_name in experiment_names:
                run_aggregation_experiments(args, dbt, agg_name)
    else:
        for agg_name in experiment_names:
            run_aggregation_experiments(args, dbt, agg_name)

    plt.show()

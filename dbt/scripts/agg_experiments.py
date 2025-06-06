from dbt.cli.main import dbtRunner, dbtRunnerResult
import matplotlib.ticker as mticker
import os
import hashlib
import numpy as np
import matplotlib.pyplot as plt
import json
import contextlib
import argparse
from tqdm import tqdm
import signal

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


def run_all_experiments(dbt, target, agg_name):
    experiment_names = experiments[agg_name]["experiment_names"]
    operation = experiments[agg_name]["operation"]

    # Warmup the database so the first experiment is not disproportionally long
    run_experiment(dbt, experiment_names[0][0], target, operation)

    execution_times: list[float] = []
    for i, variables in enumerate(tqdm(experiment_names)):
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
    else:
        # TODO Make this an actual config
        config = {
            "prob-count": {
                "filter-on-finish": False
            },
            "prob-sum": {
                "filter-on-finish": False
            }
        }
    print("Current config:")
    print(json.dumps(config, sort_keys=True, indent=4))

    hash = hashlib.md5(json.dumps(
        config, sort_keys=True).encode("utf-8")
    ).hexdigest()

    conf_dir = f"experiment_results/{target}/{agg_name}/{hash}"

    if not args.rerun and os.path.isdir(conf_dir):
        execution_times = np.load(f"{conf_dir}/execution_times.npy")
    else:
        execution_times = np.array(run_all_experiments(dbt, target, agg_name))

        if not os.path.isdir(f"experiment_results/{target}"):
            os.mkdir(f"experiment_results/{target}")
        if not os.path.isdir(f"experiment_results/{target}/{agg_name}"):
            os.mkdir(f"experiment_results/{target}/{agg_name}")
        if not os.path.isdir(f"experiment_results/{target}/{agg_name}/{hash}"):
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
    plt.title(agg_name)
    ax = fig.add_subplot(111, projection='3d')
    ax.plot_surface(X, Y, np.log10(execution_times), cmap="viridis")

    ax.set_xlabel("Variables")
    ax.set_ylabel("Alternatives per Variable")
    ax.set_zlabel("Execution Time ($\\log^{10}$)")

    ax.zaxis.set_major_formatter(mticker.FuncFormatter(log_tick_formatter))
    ax.zaxis.set_major_locator(mticker.MaxNLocator(integer=True))

    ax.set_xticks(xrange, [f"{x}" for x in xrange])
    ax.set_yticks(yrange, [f"{y}" for y in yrange])

    plt.savefig(f"{conf_dir}/3dplot.png")

    world_counts = (Y ** X).flatten()
    flat_execution_times = execution_times.flatten()

    plt.figure()
    plt.title(agg_name)
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

    args = parser.parse_args()

    dbt = dbtRunner()

    for agg_name in experiments:
        run_aggregation_experiments(args, dbt, agg_name)

    plt.show()

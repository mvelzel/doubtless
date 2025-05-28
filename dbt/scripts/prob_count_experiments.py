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


def build_dataset(dbt, target):
    cli_args = ["build", "--select",
                "+probabilistic_count_dataset", "--target", target, "--quiet"]
    dbt.invoke(cli_args)


def run_experiment(dbt, experiment_name, target):
    cli_args = [
        "run-operation", "run_prob_count_experiment",
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


def run_all_experiments(dbt, target):
    # Warmup the database so the first experiment is not disproportionally long
    run_experiment(dbt, "prob_count_1_1", target)

    execution_times: list[float] = []
    for variables in tqdm(range(1, 13)):
        execution_times.append([])
        for alternatives in tqdm(range(1, 13)):
            time = float("nan")
            try:
                time = float(run_experiment(
                    dbt, f"prob_count_{variables}_{alternatives}", target
                ))
            except Exception as e:
                print(e)
            execution_times[variables - 1].append(time)

    return execution_times


def log_tick_formatter(val, pos=None):
    return f"$10^{{{int(val)}}}$"
    # return f"{10**val:.2e}"      # e-Notation


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

    target = args.target

    dbt = dbtRunner()

    if args.build_dataset:
        print("Building dataset...")
        build_dataset(dbt, target)

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

    conf_dir = f"experiment_results/{target}/{hash}"
    if not args.rerun and os.path.isdir(conf_dir):
        execution_times = np.load(f"{conf_dir}/execution_times.npy")
    else:
        execution_times = np.array(run_all_experiments(dbt, target))

        if not os.path.isdir(conf_dir):
            os.mkdir(conf_dir)

        with open(f"{conf_dir}/config.json", "w") as f:
            json.dump(config, f)
        np.save(f"{conf_dir}/execution_times.npy", execution_times)

    print("Execution times:")
    print(execution_times)

    xrange = np.arange(1, execution_times.shape[0] + 1)
    yrange = np.arange(1, execution_times.shape[1] + 1)

    X, Y = np.meshgrid(xrange, yrange)

    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    surface = ax.plot_surface(X, Y, np.log10(execution_times), cmap="viridis")

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

    plt.show()

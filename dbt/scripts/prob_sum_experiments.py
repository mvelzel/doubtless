from dbt.cli.main import dbtRunner, dbtRunnerResult
import matplotlib.pyplot as plt
import argparse
from tqdm import tqdm
import signal


def build_dataset(dbt):
    cli_args = ["build", "--select", "+probabilistic_sum_dataset", "--quiet"]
    dbt.invoke(cli_args)


def run_experiment(dbt, experiment_name):
    cli_args = [
        "run-operation", "run_prob_sum_experiment",
        "--args", "{ experiment_name: " + experiment_name + " }",
    ]
    res: dbtRunnerResult = dbt.invoke(cli_args)

    if res.success:
        return res.result.results[0].execution_time
    else:
        if res.exception is not None:
            raise res.exception
        else:
            raise Exception(f"Running experiment {experiment_name} failed.")


def run_all_experiments(dbt):
    execution_times: list[float] = []
    for variables in tqdm(range(1, 10)):
        execution_times.append([])
        for alternatives in tqdm(range(1, 10)):
            try:
                execution_times[variables - 1].append(run_experiment(
                    dbt, f"prob_sum_{variables}_{alternatives}")
                )
            except Exception as e:
                print(e)
                break

    return execution_times


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.default_int_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--build-dataset',
        action=argparse.BooleanOptionalAction,
        help="Whether to build the dataset and its dependencies beforehand",
        default=False
    )
    args = parser.parse_args()

    dbt = dbtRunner()

    if args.build_dataset:
        build_dataset(dbt)

    execution_times = run_all_experiments(dbt)
    print(execution_times)

    plt.plot(execution_times)
    plt.savefig("plots/prob_sum_speed.png")
    plt.show()

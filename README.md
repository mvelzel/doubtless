# Doubtless - A Probabilistic Distributed Database

## dbt - Setting up experiments and experiment data
All data transformations in this repo are done by a library called [dbt](https://www.getdbt.com/). The dbt project can be found in the `/dbt` folder in this repository.


### Installing
To install dbt and all other dependencies this repository uses the python package manager called [uv](https://docs.astral.sh/uv/). Install that first and then to set up the dependencies run the following commands:

```
uv sync
```

```
uv run dbt deps
```

### Transforming the data
The dbt project supports two targets for running and storing all the data and data transformations. These are `spark` and `postgres`. The default is `spark`. The default credentials and setup for these targets can be found in `dbt/profiles.yml`.

To run all transformations against a specific target, run the following command:

```
uv run dbt run --target [postgres|spark]
```

The postgres target depends on the [DuBio](https://github.com/utwente-db/DuBio) plugin, so make sure to install that first.

The transformations depend on the `offers_english.json` file from the [WDC Dataset](https://github.com/utwente-dmb/wdc_pdb) and it assumes this is located in `~/offers_english.json`. To change this path, or if it does not work to provide a full path, edit the `dbt/macros/ingestion/ingest_wdc_data.sql` file.

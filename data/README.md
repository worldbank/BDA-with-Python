# Datasets

The datasets are too large for the repository, so this folder is empty apart from this file. Download
them and place them at the paths below. The notebooks use paths relative to their own folder
(`../../data/...`), so they work once the files are here.

> **TODO (instructor):** add a download link for each dataset marked "Course Drive".

| Path | Used in | Source |
|:--|:--|:--|
| `data/raw/hh_data.txt` | 05 `introducing-spark-apis`, 06 `working-with-spark-dataframes` | Course Drive |
| `data/raw/hh_data_ml.csv` | 06 `dataset-description-hh-data` | Course Drive |
| `data/raw/activity_log_raw.csv` | 02 `intro-to-parallelism-in-python`, 05 `hello-spark`, 05 `introducing-spark-apis`, 06 `spark-structured-apis`, 06 `working-with-spark-dataframes` | Course Drive |
| `data/raw/simulated_cdrs/` | 02 `intro-to-parallelism-in-python`, 05 `hello-spark` | Course Drive |
| `data/raw/simulated_locs.csv` | 05 `hello-spark` | Course Drive |
| `data/raw/word_count_files/` | 05 `introducing-spark-apis` | Course Drive (any folder of plain-text files works) |
| `data/raw/city.list.json` | 03 `intro-to-apis`, 06 `spark-structured-apis`, 06 `working-with-spark-dataframes` | [OpenWeather bulk download](https://bulk.openweathermap.org/sample/city.list.json.gz) (unzip it) |
| `data/raw/ISO-country-list.csv` | 03 `wiki-pages` | Course Drive |
| `data/kaggle-expedia-train.csv` | 07 `ml-with-large-datasets` | [Kaggle: Expedia Hotel Recommendations](https://www.kaggle.com/c/expedia-hotel-recommendations/data) (`train.csv`) |
| `data/sample_svm_data.txt` | 07 `ml-with-large-datasets` | [Apache Spark repository](https://github.com/apache/spark/tree/master/data/mllib) |

Some notebooks also write output here, for example `data/raw/many-csv-files/` and `data/wiki-downloads/`.
Everything in `data/` except this README is ignored by git.

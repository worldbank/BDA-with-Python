# Big Data Analytics with Python

A practical course on working with large-scale datasets in Python: functional programming and
parallelism, collecting data from the web, the Hadoop ecosystem, and data processing and machine
learning with Apache Spark. It was taught at the African Institute for Mathematical Sciences (AIMS),
Rwanda, as part of the MSc in Mathematical Sciences (Data Science stream).

**Course website: <https://worldbank.github.io/BDA-with-Python>**

## Quick start

```bash
git clone https://github.com/worldbank/BDA-with-Python.git
cd BDA-with-Python
conda env create -f environment.yml
conda activate bda
jupyter lab
```

Then download the datasets listed in [`data/README.md`](data/README.md). The full instructions,
including Java/PySpark troubleshooting, Docker and Google Colab options, are on the
[Setup page](https://worldbank.github.io/BDA-with-Python/setup.html).

Notebooks are in `notebooks/`, one folder per module (`00-intro-to-python` … `07-ml-with-spark`).

To preview the website locally, install [Quarto](https://quarto.org) and run `quarto preview`.

## License

- Course content (text, notebooks' prose, slides): [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- Code: [MIT](LICENSE)

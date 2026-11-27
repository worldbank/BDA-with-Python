# Big Data Analytics with Python

A practical course on working with large-scale datasets in Python: functional programming and
parallelism, collecting data from the web, the Hadoop ecosystem, and data processing and machine
learning with Apache Spark. It was taught at the African Institute for Mathematical Sciences (AIMS),
Rwanda, as part of the MSc in Mathematical Sciences (Data Science stream). It is designed for
public servants (economists, statisticians and data scientists in government) and data scientists
who already know how to analyse data and want to learn the techniques for large-scale datasets.

## Course website

> **Participants: start at the course website, <https://worldbank.github.io/BDA-with-Python>.**
> Everything you need to follow the course is there. This repository only holds the source files.

| What you need | Where to find it |
|---|---|
| Syllabus, schedule and assessment | [Syllabus](https://worldbank.github.io/BDA-with-Python/syllabus.html) |
| Module pages: overview, slides, notebooks, exercises | [Home page module grid](https://worldbank.github.io/BDA-with-Python/) (or the **Modules** menu on the site) |
| Lecture slides (English) | Linked from each module page and from the syllabus |
| Lecture slides (French / *version française*) | [Translated versions (SharePoint)](https://worldbankgroup.sharepoint.com/:f:/r/teams/DECGlobal-WBGroup/Shared%20Documents/General/Exchange/1.%20Courses/1.3%20-%20on-site%20Learning%20Courses/FY2027%20Courses/ENSEA%20FY27%20Courses/Course%20n.%202%20-%20Big%20Data%20Analytics/Translated%20versions?csf=1&web=1) |
| Installing Python, Java and PySpark | [Software setup](https://worldbank.github.io/BDA-with-Python/setup.html) and [PySpark setup](https://worldbank.github.io/BDA-with-Python/pyspark-setup.html) |
| Hadoop sandbox (Module 4) | [Hadoop sandbox setup](https://worldbank.github.io/BDA-with-Python/hadoop-sandbox-setup.html) |
| Datasets used in the notebooks | [`data/README.md`](data/README.md): what each file is, which notebook uses it, and where to download it |

Each notebook on the website also has an **Open in Colab** button, so you can run it in the browser
without installing anything.

## Quick start

To run the notebooks on your own machine:

```bash
git clone https://github.com/worldbank/BDA-with-Python.git
cd BDA-with-Python
conda env create -f environment.yml
conda activate bda
jupyter lab
```

Then download the datasets listed in [`data/README.md`](data/README.md) into the `data/` folder.
If anything goes wrong (Java, PySpark, Windows), see the
[setup guides](https://worldbank.github.io/BDA-with-Python/setup.html), which also
cover Docker and Google Colab.

## What's in this repository

```
notebooks/      one folder per module, 00-intro-to-python … 07-ml-with-spark (notebooks + module page)
*.qmd          website pages: home, syllabus, setup guides, resources, about
data/           datasets go here (not committed; see data/README.md)
src/            shared Python helpers
_quarto.yml     website configuration
```

The website is built from this repository with [Quarto](https://quarto.org) and published
automatically when changes are merged into `main`. To preview it locally, install Quarto and run
`quarto preview`.

## Contributing

Instructors and contributors: to add a notebook, put it in its module folder under `notebooks/`,
add it to that module's `index.qmd` and to the sidebar in `_quarto.yml`, then open a pull request.
The site is rebuilt when the pull request is merged.

## License

- Course content (text, notebooks' prose, slides): [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- Code: [MIT](LICENSE)

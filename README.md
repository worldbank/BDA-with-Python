# Big Data Analytics with Python

A practical course on working with large-scale datasets in Python: functional programming and
parallelism, collecting data from the web, the Hadoop ecosystem, and data processing and machine
learning with Apache Spark. It is designed for public servants (economists, statisticians and data
scientists in government) and for data scientists who already know how to analyse data and want to
learn the techniques for large-scale datasets.

## Course website

### 👉 **<https://worldbank.github.io/BDA-with-Python>**

**Participants: start at the website.** It's where you'll find:

- the syllabus and schedule
- a page for each module, with lecture slides (English and French), notebooks and exercises
- step-by-step setup instructions for your computer
- where to download the course datasets

This repository holds the source files for the website.

## Setting up your computer

Follow the **[setup guide](https://worldbank.github.io/BDA-with-Python/guides/software-setup.html)**
on the website. It starts from scratch (you don't need Python installed already), and a single step
installs everything the course needs: **Python, Jupyter, Java and PySpark**. You don't need to
install PySpark separately. The Hadoop sandbox for Module 4 has its own
[guide](https://worldbank.github.io/BDA-with-Python/guides/hadoop-sandbox-setup.html).

<details>
<summary><b>Already use conda (Anaconda or Miniforge)?</b> Short version</summary>

```bash
git clone https://github.com/worldbank/BDA-with-Python.git
cd BDA-with-Python
conda env create -f environment.yml   # installs Python packages, Jupyter, Java 17 and PySpark
conda activate bda
jupyter lab
```

Then open `guides/check-setup.ipynb` and run all cells to confirm everything, including Spark, works.

</details>

## Contributing

Instructors and contributors: see the
[contributing guide](https://worldbank.github.io/BDA-with-Python/guides/contributing.html).
The website is built with [Quarto](https://quarto.org) and published automatically when changes are
merged into `main`.

## License

- Course content (text, notebooks' prose, slides): [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- Code: [MIT](LICENSE)
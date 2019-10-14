# WordWideWeb

### Table of Contents
1. [Introduction](#introduction)
2. [Dataset](#dataset)
3. [Pipeline](#pipeline)
4. [Airflow](#airflow)
5. [Website](#website)

### Introduction
There are billions of webpages that existed on the Internet, with just as many more being created as the years go on. In order to anticipate which topics will be talked about in the future, one would want to know what has already been discussed. Advertisers may wish to know for the sake of promoting specific products over others. However, scraping data from webpages is an arduous and time-consuming process that may deter people.

### Dataset
![Common Crawl](https://camo.githubusercontent.com/22603dc75492b647b165e665eacccf42751ededf/687474703a2f2f636f6d6d6f6e637261776c2e6f72672f77702d636f6e74656e742f75706c6f6164732f323031362f31322f6c6f676f636f6d6d6f6e637261776c2e706e67)

A random sampling of 250 files per month were taken from 2018, where each file is about 350MB worth of text.

### Pipeline
![Pipeline](images/pipeline.png)

Data was pulled from S3, word frequencies were computed through Spark and stored into PostgreSQL with TimescaleDB configurations. The web application was built with Flask, and Airflow is used to orchestrate jobs that are run by the user to query new words.

### Airflow
![Airflow](images/airflow.png)

When the user schedules a job, a row will be added with the corresponding word and user-inputted email to a table `requests`. This will trigger an SQL sensor on the Airflow scheduler, as the table is no longer empty. The row will then be removed, and Spark jobs will be submitted in order to obtain the data for how often the word appeared. Once these Spark jobs are complete, the user will be notified via email.

### Website
![Website](images/website.png)

The website is available at [lensoftruth.me](http://lensoftruth.me).

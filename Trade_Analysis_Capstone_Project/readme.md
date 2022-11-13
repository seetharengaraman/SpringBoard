# Trade and Quote Analysis Capstone Project

## Requirements

Spring Capital is an imaginary investment bank who owe their success to
Big Data analytics. They make critical decisions about investments based
on high-frequency trading data analysis. High-frequency financial data
relate to important financial market events, like the price of a stock,
that are collected many times throughout each day.

Spring Capital collects data on trades and quotes from multiple
exchanges every day. Their data team creates data platforms and
pipelines that provide the firm with insights through merging data
points and y calculating key indicators. Spring Capital's business
analysts want to better understand their raw quote data by referencing
specific trade indicators which occur whenever their quote data is
generated, including:

-   **Latest trade price**

-   **Prior day closing price**

-   **30-minute moving average trade price** (Average price over the
    past 30 minutes, constantly updated. This is a common indicator
    which smooths the price trend and cuts down noise.) As a data
    engineer, we will build a data pipeline that produces a dataset
    including the above indicators for the business analysts.

The goal of this project is to build an end-to-end data pipeline to
ingest and process daily stock market data from multiple stock
exchanges. The pipeline should maintain the source data in a structured
format, organized by date. It also needs to produce analytical results
that support business analysis.

**Technical Requirements**

This project can be implemented using Spark/Hadoop. Trade and quote data
from each exchange could contain billions of records. The pipeline needs
to be scalable enough to handle that. Use a cloud elastic cluster
service, to handle variant data volume across different days.

**Data Source**

The source data used in this project is randomly generated stock
exchange data.

**Trades**: records that indicate transactions of stock shares between
broker-dealers. See trade data below.

**Quotes**: records of updates best bid/ask price for a stock symbol on
a certain exchange. See quote data below.

**Trade data:**

  **Column**              **Type**
  ----------------------- ------------
  Trade Date              Date
  Record Type             Varchar(1)
  Symbol                  String
  Execution ID            String
  Event Time              Timestamp
  Event Sequence Number   Int
  Exchange                String
  Trade Price             Decimal
  Trade Size              Int

**Quote data:**

  **Column**              **Type**
  ----------------------- ------------
  Trade Date              Date
  Record Type             Varchar(1)
  Symbol                  String
  Event Time              Timestamp
  Event Sequence Number   Int
  Exchange                String
  Bid Price               Decimal
  Bid Size                Int
  Ask Price               Decimal
  Ask Size                Int

## Flow Diagram

![](media/image1.png){width="6.5in" height="1.979861111111111in"}

## 

## 

## Architecture

![](media/image2.png){width="6.5in" height="3.6756944444444444in"}

## Detailed Design

**Design 1:**

Spin up Helm chart based Apache Airflow in Google Kubernetes Cluster,
build docker image consisting of the dags and required Google Cloud
operators, apply to the cluster. Subsequently run a pipeline from
Airflow UI on Kube Cluster(exposed through Load Balancer config) to:

a.  Create Data proc cluster on the fly using
    DataprocCreateClusterOperator

b.  Perform Ingestion of Trade and Quote data from Google Cloud Storage
    along with End of Day Duplicate removal corrections as a pyspark job
    using DataprocSubmitJobOperator

c.  Perform Enrichment of Trade and Quote data for Business Analysis
    requirements as a pyspark job using DataprocSubmitJobOperator

d.  Delete the Dataproc cluster using DataprocDeleteClusterOperator

**References used:**

<https://airflow.apache.org/docs/helm-chart/stable/index.html>

<https://airflow.apache.org/docs/docker-stack/build.html#>

<https://towardsdatascience.com/deploying-airflow-on-google-kubernetes-engine-with-helm-28c3d9f7a26b>

<https://medium.com/towards-data-science/deploying-airflow-on-google-kubernetes-engine-with-helm-part-two-f833b0a3b0b1>

**Issues faced:**

1.  Workload Identity setup was needed -
    <https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity>

2.  Task received sigterm signal owing to pods getting deleted.
    Subsequent airflow task retries did complete the step

3.  Helm upgrade sometimes failed with the custom docker image added.
    Troubleshooting was time consuming

4.  Lot of memory and cpu consumption as well within the Kubernetes
    cluster for setting up the Helm based Airflow followed by running of
    the pipeline

Owing to the issues, switched to Design 2

**Design 2:**

1.  Cloud Composer -- Apache Airflow for pipeline orchestration and Web
    based graphic representation of the pipeline along with logs

2.  As part of Apache Airflow pipeline, Serverless Dataproc creates
    batches for PySpark job processing (both for ingestion of data from
    Cloud storage and enrichment of the same and storing results in
    parquet format back to Cloud storage). Once processing is complete,
    batches are deleted

    a.  Two limitations:

        i.  Pyspark file should be named as "spark-job.py" mandatorily

        ii. Hence, If more than one pyspark job need to be run within
            same pipeline, each needs to be placed in its own bucket (no
            additional folders possible)

3.  Single node Dataproc cluster for Persistent History storage and
    display Spark Job details on UI (including dags and execution plans)

4.  Serverless Dataproc internally creates a Kubernetes cluster to
    dynamically spin up needed pods to process the spark jobs

**Reference:**

<https://cloud.google.com/composer/docs/composer-2/run-dataproc-workloads>

**Future considerations:**

Trigger a cloud function based on files added to cloud storage which
will internally:

1.  Spin up Cloud Composer instance

2.  Spin up single node Dataproc cluster for Persistent History Storage
    (PHS) (with configuration to shut down if no activity for 30 min)

3.  Run the pipeline

4.  Delete Cloud Composer instance

## Steps to run

1.  Follow link under "Reference" above to start Cloud composer instance
    and run the pipeline

2.  Ensure to place the data and jar files in the same way it is stored
    in the repository within the ingest_trade_quote and
    trade_quote_analysis folders.

3.  Setup below variables under Airflow variables

    a.  project_id (GCP Project Id)

    b.  region_name (GCP region)

    c.  ingest_bucket_name - ingest_trade_quote

    d.  process_bucket_name - trade_quote_analysis

    e.  phs_cluster (name of single node data proc cluster)

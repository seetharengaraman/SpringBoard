# Save Soil Analysis Capstone Project

## Requirements

The UN Convention to Combat Desertification (UNCCD) has
[reported](https://www.unccd.int/news-events/world-soil-day-2020-keep-soil-alive-protect-biodiversity)
that, unless we rapidly change how we manage our soils, 90% of it could
become degraded by 2050 and result in a global food crisis. To avert
this impending crisis and restore soil health, the Save Soil movement,
initiated by Sadhguru,aims to increase organic content in soil to a
minimum of 3-6% through policy changes across all nations. We are at the
cusp of restoring soil health in the next 15-20 years if we take the
right action now.Â  Major amount of soil can be restored by ensuring
sustainable farming practices.

![alt text](
https://github.com/seetharengaraman/SpringBoard/blob/main/Capstone%20-%20Save%20Soil%20Implementation%20Support/Soil%20Degradation.png)

![alt text](
https://github.com/seetharengaraman/SpringBoard/blob/main/Capstone%20-%20Save%20Soil%20Implementation%20Support/Save%20Soil1.png)

![alt text](
https://github.com/seetharengaraman/SpringBoard/blob/main/Capstone%20-%20Save%20Soil%20Implementation%20Support/Save%20Soil2.png)

Hence this project aims at studying US Agricultural Land Use and
determining:

1.  Production practices across different types of farms in US

2.  Farms using Organic vs Conventional production practices (affecting
    soil's organic content). Conventional includes heavy use of
    fertilizers and heavy tillage. Organic includes the use of animal
    and plant waste as manure, reduced tillage, cover crops and letting
    animals graze on fields amongst other things

3.  Current soil organic matter representative value and percentiles
    across various states in US and find the correlation between farming
    practices used in the state and its corresponding organic matter
    changes

This will help determine farms in states with best practices which can
be used to influence soil policies to help increase organic content in
the soil.

Additionally, world level soil organic matter across different countries
are also captured as part of this project to be measured every year and
compared to share best practices across countries.

Data pipeline built to measure this every year with new
survey/census/carbon content data being available to measure the organic
content increase across the farmlands.

**Technical Requirements**

This project can be implemented using Spark/Hadoop. When number of map
units where soil surveys are made increases or number of surveys
increases, it could contain billions of records. The pipeline needs to
be scalable enough to handle that. Use a cloud elastic cluster service,
to handle variant data volume.

**Data Source:**

Data fetched from reliable sites - Government (USDA) and United Nations
(FAO).

US Agriculture Land Use data (USDA):

**Source :**

[https://www.nass.usda.gov/Quick_Stats/]{.ul}

<https://quickstats.nass.usda.gov/api>

All production practices filter (used in postman):
GET http://quickstats.nass.usda.gov/api/api_GET/?key={{API_KEY}}&source_desc=CENSUS&sector_desc=ECONOMICS&short_desc=AG%20LAND,%20CROPLAND%20-%20ACRES&domain_desc=TOTAL&agg_level_desc=STATE

Organic production practices filter:
GET http://quickstats.nass.usda.gov/api/api_GET/?key={{API_KEY}}&source_desc=CENSUS&sector_desc=ECONOMICS&short_desc=AG%20LAND,%20CROPLAND,%20ORGANIC%20-%20ACRES&domain_desc=ORGANIC%20STATUS&agg_level_desc=STATE



US Soil Organic Matter data (USDA):

**Source:** <https://sdmdataaccess.nrcs.usda.gov/>

<https://sdmdataaccess.nrcs.usda.gov/Query.aspx>

Sample query:
SELECT saverest [Date Added],l.areasymbol [Area Symbol], l.areaname [Area Name],muname [Map Unit Name], compname [Component Name], comppct_r [Component Representative Percentage], ch.hzname [Horizon Name],CONCAT(hzdept_r * 0.39,'-',hzdepb_r * 0.39) [Depth in Inches],ch.om_l [Organic Matter Low],ch.om_r [Organic Matter RV],ch.om_h [Organic Matter High]ÊÊ
FROM sacatalog sacÊ
INNER JOIN legend l ON l.areasymbol = sac.areasymbol AND l.areasymbol IN ('MD601')Ê
INNER JOIN mapunit mu ON mu.lkey = l.lkeyÊ
LEFT OUTER JOIN component c ON c.mukey = mu.mukeyÊ
LEFT OUTER JOIN chorizon ch ON ch.cokey = c.cokey



World Level Harmonized Soil Data (FAO):

**Source:**
<https://www.fao.org/soils-portal/data-hub/soil-maps-and-databases/harmonized-world-soil-database-v12/en/>

World Countries Map:

**Source:**
https://hub.arcgis.com/datasets/esri::world-countries-generalized/explore?location=-0.458459%2C0.000000%2C2.88&showTable=true

**Destination:**
https://console.cloud.google.com/

Big Query Tables (with Raw and transformed data):
![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Capstone%20-%20Save%20Soil%20Implementation%20Support/Raw%20and%20Transformed%20Data%20Tables.png)



## Flow Diagram

![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Capstone%20-%20Save%20Soil%20Implementation%20Support/Save%20Soil%20Flow%20Diagram.png)

## 

## Architecture

![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Capstone%20-%20Save%20Soil%20Implementation%20Support/Save%20Soil%20Implementation%20Architecture.png)

## Detailed Design

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

**Visualization:**

US Data:

[**https://public.tableau.com/views/OrganicvsAllProductionPracticesAreainAcresbyUSState/OrganicvsAllProductionPracticeFarms?:language=en-US&:display_count=n&:origin=viz_share_link**](https://public.tableau.com/views/OrganicvsAllProductionPracticesAreainAcresbyUSState/OrganicvsAllProductionPracticeFarms?:language=en-US&:display_count=n&:origin=viz_share_link)

[**https://public.tableau.com/views/USStatewiseSoilOrganicMatterProfile/USStatewiseSoilOrganicMatterProfile?:language=en-US&:display_count=n&:origin=viz_share_link**](https://public.tableau.com/views/USStatewiseSoilOrganicMatterProfile/USStatewiseSoilOrganicMatterProfile?:language=en-US&:display_count=n&:origin=viz_share_link)

World Data: 
[**https://public.tableau.com/views/WorldSoilOrganicCarbonProfilebySoilType/Dashboard1?:language=en-US&:display_count=n&:origin=viz_share_link**](https://public.tableau.com/views/WorldSoilOrganicCarbonProfilebySoilType/Dashboard1?:language=en-US&:display_count=n&:origin=viz_share_link)


![alt text](
<https://github.com/seetharengaraman/SpringBoard/blob/main/Capstone%20-%20Save%20Soil%20Implementation%20Support/Total%20Acres.png>)

![alt text](
https://github.com/seetharengaraman/SpringBoard/blob/main/Capstone%20-%20Save%20Soil%20Implementation%20Support/Organic%20Farm%20Expansion.png)

![alt text](
https://github.com/seetharengaraman/SpringBoard/blob/main/Capstone%20-%20Save%20Soil%20Implementation%20Support/Organic%20Matter%20profile.png)


![alt text](
https://github.com/seetharengaraman/SpringBoard/blob/main/Capstone%20-%20Save%20Soil%20Implementation%20Support/World%20soil%20data%20visualization.png)



**Future considerations:**

**Functional:**

Possible future directions can be into determining drought affected
states and reviewing the farm practices and determining how these can be
turned around with increasing organic content in soil. Additionally
climatic conditions and soil textures can be added as features to the
data available to profile and model farms with best practices aided by
right climatic conditions and rich in organic content to be replicated
at other parts of US as well as across the world under similar
conditions.

**Technical:**

Trigger a cloud function based on files added to cloud storage which
will internally:

1.  Spin up Cloud Composer instance

2.  Spin up single node Dataproc cluster for Persistent History Storage
    (PHS) (with configuration to shut down if no activity for 30 min)

3.  Run the pipeline

4.  Delete Cloud Composer instance

More Unit testing along with test scripts to be added

## Steps to run

1.  Follow link under "Reference" above to start Cloud composer instance
    and run the pipeline

2.  Ensure to place the data, python file and jar files in the same way
    it is stored in the repository within the various folders. Only
    sample data is present in current repository

3.  Setup below variables under Airflow variables

    a.  project_id (GCP Project Id)

    b.  region_name (GCP region)

    c.  dataset_id (Big Query Dataset Id)

    d.  agri_bucket_name -- ingest_agriculture_data

    e.  us_soil_bucket_name- soil_organic_matter

    f.  world_soil_bucket_name - world_soil_organic_matter

    g.  phs_cluster (name of single node data proc cluster)


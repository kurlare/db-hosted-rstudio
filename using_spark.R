
## Setup ##

## One option is to specify DBFS as your working directory
setwd("/dbfs/rk")

## Load libraries
library(sparklyr)
library(dplyr)

## Reading local files on DBFS is easy
wine <- read.csv("/dbfs/FileStore/tables/wine_quality-ca9ce.csv", stringsAsFactors = F)

## Now we can work with data as we normally would
View(wine)

## Working with Spark is a snap, too.
SparkR::sparkR.session()

## sparklyr requires creating a connection to Spark first
sc <- spark_connect(method = "databricks")

## Read airlines dataset from 2008
airlinesDF <- SparkR::read.df("/databricks-datasets/asa/airlines/2008.csv", 
                              source = "csv", 
                              inferSchema = "true", 
                              header = "true")

## In sparklyr we read data from 2007
sparklyAirlines <- spark_read_csv(sc,
                                  name = 'airlines',
                                  path = "/databricks-datasets/asa/airlines/2007.csv")

carrierCounts <- group_by(sparklyAirlines, UniqueCarrier) %>% 
  count() %>%
  arrange(desc(n))

head(carrierCounts)

## To persist files, write to DBFS
sparklyr::spark_write_csv(carrierCounts,
                          path = "/rk/data/carrierCounts.csv")

## To persist scripts, also write to DBFS
system("cp using_spark.R /dbfs/rk/scripts")

## Make sure it is there!
system("ls /dbfs/rk/scripts")

## Making an update!



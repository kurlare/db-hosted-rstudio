
## Setup ##

## One option is to specify DBFS as your working directory
setwd("/dbfs/rk")

## Load libraries
SparkR::sparkR.session()
library(sparklyr)
library(dplyr)
sc <- spark_connect(method = "databricks")

#suppressMessages(library(SparkR))
#suppressMessages(library(magrittr))

## sparklyr requires creating a connection to Spark first
sc <- spark_connect(method = "databricks")

## Read airlines dataset from 2008
airlinesDF <- SparkR::read.df("/databricks-datasets/asa/airlines/2008.csv", source = "csv", inferSchema = "true", header = "true")

# If file was in parquet format
# airlinesDF <- read.parquet("/databricks-datasets/asa/airlines/2008/")

## In sparklyr we specify the spark connection, read data from 2007
sparklyAirlines <- sparklyr::spark_read_csv(sc, name = 'airlines', path = "/databricks-datasets/asa/airlines/2007.csv")

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



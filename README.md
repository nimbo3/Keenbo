# Keenbo [![Build Status](https://travis-ci.com/nimbo3/Keenbo.svg?branch=master)](https://travis-ci.com/nimbo3/Keenbo) [![codecov.io](https://codecov.io/github/nimbo3/Keenbo/coverage.svg?branch=master)](https://codecov.io/github/nimbo3/Keenbo?branch=master) [![Coverage](https://sonarcloud.io/api/badges/measure?key=nimbo3_Keenbo&metric=coverage)](https://sonarcloud.io/component_measures?id=nimbo3_Keenbo&metric=coverage)

A **simple to use** search engine for search data.


Project consist of different modules:
* crawler: crawls the pages and save in database
* search-engine: search API on stored data
* forward-extractor: run a mapreduce for calculate incomming link for a website to have better results

## Getting Started

For run this project on your own local machine or server you should install [Zookeeper](https://github.com/nimbo3/Keenbo/wiki/Setup-Zookeeper-on-cluster) and [hbase](https://github.com/nimbo3/Keenbo/wiki/Setup-HBase-1.2.4-in-multi-node-cluster) and [hadoop](https://github.com/nimbo3/Keenbo/wiki/Setup-Hadoop-2.7.7-in-multi-node-cluster) and [elasticsearch](https://github.com/nimbo3/Keenbo/wiki/Setup-ElasticSearch) and [kafka](https://github.com/nimbo3/Keenbo/wiki/Setup-Kafka-on-cluster ).

### Prerequisites

For installing dependencies for this project, read [wikis](https://github.com/nimbo3/Keenbo/wiki) and configure it properly depend on your servers.

### Running

For running application, you can use `.sh` files inside `bin` folder.

## Running the tests

Test all projects with below command: 
```
mvn test
```

## Built With

* Spark - Used to run mapreduces
* Kafka - Used to handling links queue 
* ElasticSearch - Used to run search queries
* Redis - Used to check duplicated pages
* HBase - Used to store data 
* DropWizard - Used to monitoring
* JSoup - Used to parse the pages
* Caffeine - Used to store requested urls to send request politely
* Jackson - Used to serializing objects
* Maven - Used to Dependency Management

## Authors
* **Amin Borjian** - [github](https://github.com/Borjianamin98)
* **Danial Erfanian** - [github](https://github.com/DanialErfanian)
* **Ehsan Karimi** - [github](https://github.com/karimiehsan90)
* **MohammadReza Pakzadian** - [github](https://github.com/mrp-78)


See also the list of [contributors](https://github.com/nimbo3/Keenbo/graphs/contributors) who participated in this project.

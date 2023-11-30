# Astronomer Field Engineer Panel Interview: Jake Roach

Welcome! This repo contains all the materials prepared for the Astronomer Field Engineer Panel Interview, for Jake 
 Roach. Three DAGs were written, each meeting the requirements outlined in the 
 `Astronomer Field Engineering Exercise.pdf` file in the root of this repository. These DAGs were tested in a local, 
 development Airflow environment created used the Astro CLI. For more details about the development and testing process, 
 please see the respective **Environment Creation**, **Development**, and **Testing** sections below. A high-level 
 overview of the entire process and obstacles encountered are also detailed below. Please feel free to reach out to Jake
 Roach via email (jroachgolf84@outlook.com) with any questions about the contents of this repository.

## Table of Contents
TODO: Make sure to add a table of contents here

## Overview
TODO: Add an overview at the end of the project

## Environment Creation

To create an Astro project, the Astro CLI is the best tool to use. This was already installed on  my local machine, 
 which I validated with the following command:

```commandline
> astro version
```

Within the root of this repository (`astronomer-panel-interview`), I initialized an Astro project with the command 
 below. I'm using the latest version of the Astro Runtime (9.5.0), which I specified in the command.

```commandline
> astro dev init --runtime-version 9.5.0
```
The appropriate files were created at the root of this repository, and I validated that my Astro project could be run
 (locally) by running the command `astro dev start`. After about a minute, my default web browser opened to 
 `localhost:8080`, and I was able to log into the Airflow UI. Success!

TODO: Add points about cleaning up the default "example" DAGs

## Development

### Market ETL
The first DAG that I built was a basic ETL pipeline which  market data from the Polygon API, flattened the JSON that was
 returned in the response, transformed the flattened data, and loaded the data to a Postgres database. First, this DAG 
 was implemented using "traditional" Airflow operators, and then later, with the TaskFlow API. To configure the API key 
 that I'd be using to pull data from the Polygon API, I ran the following command: 

```commandline
> astro dev run variables set POLYGON_API_KEY *****
```

To create a Postgres connection, I ran the following:

```commandline
> astro dev run connections add \
    --conn-type postgres \
    --conn-host jroachgolf84-sandbox-postgres.ciz3ssohle2n.us-east-1.rds.amazonaws.com \
    --conn-login jroachgolf84 \
    --conn-password '*****' \
    --conn-port 5432 \
    --conn-schema postgres \
    postgres_market_conn
```

The table was initially created with the command below. This allowed for idempotency to be built into the pipeline (the
 `DELETE` command before the DataFrame is appended to the table each run). See below:

``` {sql}
CREATE TABLE market.transformed_market_data (
	market_date TEXT,
	ticker TEXT,
	open_price FLOAT,
	high_price FLOAT,
	low_price FLOAT,
	close_price FLOAT,
	change FLOAT,
	volume FLOAT
);
```

## Testing

## Obstacles

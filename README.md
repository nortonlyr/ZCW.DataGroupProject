### ZCW.DataGroupProject

![description_if_image_fails_to_load](https://github.com/nortonlyr/ZCW.DataGroupProject/blob/master/final_project_050620.jpg)

NON-TRADITIONAL HOUSING PRICE PREDICTOR
-------------------

This repo contains the code for a collaborative project between 3- Zip Code Wilmington students, that have explored both static and streaming data to find out what are the relevant factors that are driving the Philadelphia City housing market.

### THE QUESTION: 
What are the factors that currently driving housing prices in Philadelphia?

### DATA SOURCE 
 We will collect some historical data from Open Databsae of Philly, including real estate, public schools, hospitals, crime rate, parks and recreation facilities, etc.

### ETL PIPELINE 
We will use Airflow and python to get request the data from Database, for daily streaming data, we will try to use real estate API or web scarping tool to obtain.

Data obtained each source will have be stored into a SQL database

### CLEANING OF DATA 
Apache Spark will be applied for the data wrangle. Then the processed data will to stored into a new SQL database. 

### HOUSING PREDICTION 
Jupyter notebook will be used to load the data with applying SQL query for data analysis and pridction model building. The data visualization will be also performed with matplotlib and other data visulization tools. 

 Also, we will update some daily housing information in the Philly area, to allow us to have a parallel view of the price prediction study. A streaming API data will be added


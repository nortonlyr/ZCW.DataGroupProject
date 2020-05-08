### ZCW.DataGroupProject

In this group porject, our topic is housing price prediction of Philadelphia city area. We will collect some historical data from Open Databsae of Philly, including real estate, publica school, hospital, crime rate, park and recreation facilities, and so on. Also, we will update some daily housing information in the Philly area, to allow us to have a parallel view of the price prediction study. A streaming API data will be added.

We will use airflow and python to get request the data from Database, for daily streaming data, we will try to use real estate API or web scarping tool to obtain.

Data obtained each source will have be stored into a SQL database

Apache Spark will be applied for the data wrangle. Then the processed data will to stored into a new SQL database. 

Jupyter notebook will be used to load the data with applying SQL query for data analysis and pridction model building. The data visualization will be also performed with matplotlib and other data visulization tools. 



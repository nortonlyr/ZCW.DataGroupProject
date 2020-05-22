# TRANSFORMATION (CLEANING, STRUCTURE, SUMMARY)


The most important question of any project is: how clean is your [data](https://github.com/nortonlyr/ZCW.DataGroupProject/blob/master/jupyter_notebook/phi_housing_pp_main_data_analysis.ipynb)?




Data mining, cleaning, and extraction (Jupyter Notebook, Spark)
    a. Query the data from part 1 by using SQL query into Jupyter or Spark, clean the unused column, reorganize the data types if needed.
    b. Summarize the important features from the main datasets that might affect house price, for example (market_value, number_of_bedrooms, category_type, taxable_building, taxable_land), Group data by using zip code (Spark-SQL). Show summary data visualization
    c. Joining All table together by (MySQL, or Spark-SQL). This new joint table will be applied in the data visualization dash-board or performance (Tableau, plotly, bokeh)

TOOLS: 
- JUPYTER NB, SPARKSQL, DATABRICKS, PYSPARK

.  get request data from Open Database of Philly (Apache Airflow) 

    a. house public properties (main datasets to focus (more than 500k row, and 70 columns))
        - house related location data, such as Philly public school, Philly healthcare service, crime rate, police stations, etc)(total 9 datasets, preliminary clean some unknown data via airflow)
    b.  API yahoo finance data from housing index stock (real estate companies ETF) (airflow)
        - This data will be use to compare the housing price trend data from part house public properties
    c. Housing market data from Melissa.com real estate website.(download and use airflow DAGs to combine)

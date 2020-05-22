### DATA SOURCE 

We set out to find housing data that would help solve our inquiry. There were certain aspects of the the real estate of which we were uninformed. 

The real estate industry is big. According to federal statistics, the industry contributed more than $2.7 trillion to the U.S. economy in 2018 or about 13 percent of GDP. It employed more than 2 million people and generated more $10 billion in corporate profits.

Although there are numerous APIs that deal wiht housing data, a majority of it comes at a prirce. 

The solution for reliable data came via [melissa.com](https://www.melissa.com).

Melissa helps companies to harness Big Data, legacy data, and people data (names, addresses, phone numbers, and emails).

Now if we only had a [Directed Acyclic Graph](https://github.com/nortonlyr/ZCW.DataGroupProject/tree/master/dags) that would be able to pull the data that we would need from [Melissa](https://github.com/nortonlyr/ZCW.DataGroupProject/blob/master/dags/house.py)

Historical data from [Open Databsae of Philly](https://github.com/nortonlyr/ZCW.DataGroupProject/blob/master/dags/phi_data_combined.py), including real estate, public schools, hospitals, crime rate, parks and recreation facilities, etc.

Now if we could only find some nice healthy streaming data, something that would allow us to use the yahoo VQN (ticker) stock price trend data to compare to the housing price average data; maybe something like [Yahoo Finance API](https://github.com/nortonlyr/ZCW.DataGroupProject/blob/master/dags/yahoo_vnq.py).
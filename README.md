1. A README.md file with a short markdown description of this assignment and areference to the Data Expo 2009 data, and the Harvard Dataverse [1 point]

Download the Data Expo 2009 data from the Harvard Dataverse [https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/HG7NV] and construct an
SQLite Database called airline2.db with the following tables:
1. ontime (with the data from 2000 to 2005 – including 2000 and 2005)
2. airports (with the data in airports.csv)
3. carriers (with the data in carriers.csv)
4. planes (with the data in plane-data.csv)

2. A folder called “r_sql” with a R code for constructing the database (from the csvinputs) and replicating the queries in the practice quiz. The latter should be done
both using DBI and dplyr notation [1.75 points each]
- construct_database.R
- queries_dbi.R
- queries_dplyr.R
  
3. A folder called “python_sql” with a Python version of the code in point 2, basedon sqlite3 [1.75 points]
- construct_database.py
-  queries_sqlite3.py
- simplified_q4.py
  

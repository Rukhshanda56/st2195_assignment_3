import sqlite3
import pandas as pd

conn = sqlite3.connect("/Users/rukhshandaashraf/Desktop/st_2195/st2195_assignment_3_database/dataverse_files/airline.db")

query_q5_simplified = """
SELECT carriers.Description,
    ROUND(SUM(CASE WHEN ontime.Cancelled = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS cancelled_rate
FROM ontime
JOIN carriers
    ON ontime.UniqueCarrier = carriers.Code
WHERE carriers.Description IN ('United Air Lines Inc.','American Airlines Inc.','Pinnacle Airlines Inc.','Delta Air Lines Inc.')
GROUP BY carriers.Description
ORDER BY cancelled_rate DESC;
"""

result_q5_simplified = pd.read_sql_query(query_q5_simplified, conn)
print(result_q5_simplified)
result_q5_simplified. to_csv("/Users/rukhshandaashraf/Desktop/st_2195/st2195_assignment_3/python_sql/q5_simplified_output.csv")

conn.close()
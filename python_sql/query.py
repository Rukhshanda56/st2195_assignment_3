import sqlite3
import pandas as pd

conn = sqlite3.connect("/Users/rukhshandaashraf/Desktop/st_2195/st2195_assignment_3_database/dataverse_files/airline.db")

query = """
SELECT planes.model,
    AVG(ontime.DepDelay) AS avg_dep_delay
FROM ontime
JOIN planes
    ON ontime.TailNum = planes.tailnum
WHERE ontime.Cancelled = 0
    AND ontime.Diverted = 0
    AND ontime.DepDelay IS NOT NULL
    AND planes.model IN ('737-230','ERJ 190-100 IGW','A330-223','737-282')
GROUP BY planes.model
ORDER BY avg_dep_delay ASC;
"""

result = pd.read_sql_query(query, conn)
print(result)
result. to_csv("/Users/rukhshandaashraf/Desktop/st_2195/st2195_assignment_3/python_sql/q1_output.csv")

query_q3 = """
SELECT airports.city,
    COUNT(*) AS num_arrivals
FROM ontime
JOIN airports
    ON ontime.Dest = airports.iata
WHERE ontime.Cancelled = 0
    AND airports.city IN ('Chicago','Atlanta','New York','Houston')
GROUP BY airports.city
ORDER BY num_arrivals DESC;
"""

result_q3 = pd.read_sql_query(query_q3, conn)
print(result_q3)
result_q3. to_csv("/Users/rukhshandaashraf/Desktop/st_2195/st2195_assignment_3/python_sql/q3_output.csv")

query_q4 = """
SELECT carriers.Description,
    COUNT(*) AS num_cancelled
FROM ontime
JOIN carriers
    ON ontime.UniqueCarrier = carriers.Code
WHERE ontime.Cancelled = 1
    AND carriers.Description IN ('United Air Lines Inc.','American Airlines Inc.','Pinnacle Airlines Inc.','Delta Air Lines Inc.')
GROUP BY carriers.Description
ORDER BY num_cancelled DESC;
"""

result_q4 = pd.read_sql_query(query_q4, conn)
print(result_q4)
result_q4. to_csv("/Users/rukhshandaashraf/Desktop/st_2195/st2195_assignment_3/python_sql/q4_output.csv")

query_q5 = """
WITH flight_summary AS (
    SELECT
        carriers.Description AS airline,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN ontime.Cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_flights
    FROM ontime
    JOIN carriers
        ON ontime.UniqueCarrier = carriers.Code
    WHERE carriers.Description IN (
        'United Air Lines Inc.',
        'American Airlines Inc.',
        'Pinnacle Airlines Inc.',
        'Delta Air Lines Inc.'
    )
    GROUP BY carriers.Description
)
SELECT
    airline,
    total_flights,
    cancelled_flights,
    ROUND(1.0 * cancelled_flights / total_flights, 4) AS cancel_rate
FROM flight_summary
ORDER BY cancel_rate DESC;
"""

result_q5 = pd.read_sql_query(query_q5, conn)
print(result_q5)
result_q5. to_csv("/Users/rukhshandaashraf/Desktop/st_2195/st2195_assignment_3/python_sql/q5_output.csv")
conn.close()
    

library(DBI)
library(RSQLite)
library(readr)

# Connect to the SQLite database
conn <- dbConnect(RSQLite::SQLite(), "/Users/rukhshandaashraf/Desktop/st_2195/st2195_assignment_3_database/dataverse_files/airline.db")

# Query 1: Average Departure Delay by Plane Model
query1 <- "
SELECT planes.model,
       AVG(ontime.DepDelay) AS avg_dep_delay
FROM ontime
JOIN planes ON ontime.TailNum = planes.tailnum
WHERE ontime.Cancelled = 0
  AND ontime.Diverted = 0
  AND ontime.DepDelay IS NOT NULL
  AND planes.model IN ('737-230','ERJ 190-100 IGW','A330-223','737-282')
GROUP BY planes.model
ORDER BY avg_dep_delay ASC;
"
result1 <- dbGetQuery(conn, query1)
write_csv(result1, "~/Desktop/st_2195/st2195_assignment_3/r_sql/q1_output.csv")

# Query 3: Number of Arrivals by City
query3 <- "
SELECT airports.city,
       COUNT(*) AS num_arrivals
FROM ontime
JOIN airports ON ontime.Dest = airports.iata
WHERE ontime.Cancelled = 0
  AND airports.city IN ('Chicago','Atlanta','New York','Houston')
GROUP BY airports.city
ORDER BY num_arrivals DESC;
"
result3 <- dbGetQuery(conn, query3)
write_csv(result3, "~/Desktop/st_2195/st2195_assignment_3/r_sql/q3_output.csv")

# Query 4: Cancelled Flights by Airline
query4 <- "
SELECT carriers.Description,
       COUNT(*) AS num_cancelled
FROM ontime
JOIN carriers ON ontime.UniqueCarrier = carriers.Code
WHERE ontime.Cancelled = 1
  AND carriers.Description IN (
    'United Air Lines Inc.',
    'American Airlines Inc.',
    'Pinnacle Airlines Inc.',
    'Delta Air Lines Inc.'
  )
GROUP BY carriers.Description
ORDER BY num_cancelled DESC;
"
result4 <- dbGetQuery(conn, query4)
write_csv(result4, "~/Desktop/st_2195/st2195_assignment_3/r_sql/q4_output.csv")

# Query 5: Cancel Rate by Airline
query5 <- "
WITH flight_summary AS (
    SELECT carriers.Description AS airline,
           COUNT(*) AS total_flights,
           SUM(CASE WHEN ontime.Cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_flights
    FROM ontime
    JOIN carriers ON ontime.UniqueCarrier = carriers.Code
    WHERE carriers.Description IN (
        'United Air Lines Inc.',
        'American Airlines Inc.',
        'Pinnacle Airlines Inc.',
        'Delta Air Lines Inc.'
    )
    GROUP BY carriers.Description
)
SELECT airline,
       total_flights,
       cancelled_flights,
       ROUND(1.0 * cancelled_flights / total_flights, 4) AS cancel_rate
FROM flight_summary
ORDER BY cancel_rate DESC;
"
result5 <- dbGetQuery(conn, query5)
write_csv(result5, "~/Desktop/st_2195/st2195_assignment_3/r_sql/q5_output.csv")

# Disconnect
dbDisconnect(conn)
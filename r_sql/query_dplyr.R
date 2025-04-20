library(DBI)
library(RSQLite)
library(dplyr)
library(readr)

# Connect to the SQLite database
conn <- dbConnect(RSQLite::SQLite(), "/Users/rukhshandaashraf/Desktop/st_2195/st2195_assignment_3_database/dataverse_files/airline.db")

# Reference tables
ontime <- tbl(conn, "ontime")
planes <- tbl(conn, "planes")
airports <- tbl(conn, "airports")
carriers <- tbl(conn, "carriers")

# Query 1: Average Departure Delay by Plane Model
result1 <- ontime %>%
  inner_join(planes, by = c("TailNum" = "tailnum")) %>%
  filter(Cancelled == 0, Diverted == 0, !is.na(DepDelay), 
         model %in% c("737-230", "ERJ 190-100 IGW", "A330-223", "737-282")) %>%
  group_by(model) %>%
  summarise(avg_dep_delay = mean(DepDelay, na.rm = TRUE)) %>%
  arrange(avg_dep_delay) %>%
  collect()

write_csv(result1, "~/Desktop/st_2195/st2195_assignment_3/r_sql/q1_output.csv")

# Query 3: Number of Arrivals by City
result3 <- ontime %>%
  inner_join(airports, by = c("Dest" = "iata")) %>%
  filter(Cancelled == 0, city %in% c("Chicago", "Atlanta", "New York", "Houston")) %>%
  group_by(city) %>%
  summarise(num_arrivals = n()) %>%
  arrange(desc(num_arrivals)) %>%
  collect()

write_csv(result3, "q3_output.csv")

# Query 4: Cancelled Flights by Airline
result4 <- ontime %>%
  inner_join(carriers, by = c("UniqueCarrier" = "Code")) %>%
  filter(Cancelled == 1,
         Description %in% c("United Air Lines Inc.", "American Airlines Inc.", 
                            "Pinnacle Airlines Inc.", "Delta Air Lines Inc.")) %>%
  group_by(Description) %>%
  summarise(num_cancelled = n()) %>%
  arrange(desc(num_cancelled)) %>%
  collect()

write_csv(result4, "~/Desktop/st_2195/st2195_assignment_3/r_sql/q4_output.csv")

# Query 5: Cancel Rate by Airline
flight_summary <- ontime %>%
  inner_join(carriers, by = c("UniqueCarrier" = "Code")) %>%
  filter(Description %in% c("United Air Lines Inc.", "American Airlines Inc.", 
                            "Pinnacle Airlines Inc.", "Delta Air Lines Inc.")) %>%
  group_by(airline = Description) %>%
  summarise(
    total_flights = n(),
    cancelled_flights = sum(Cancelled == 1)
  ) %>%
  mutate(cancel_rate = round(cancelled_flights / total_flights, 4)) %>%
  arrange(desc(cancel_rate)) %>%
  collect()

write_csv(flight_summary, "~/Desktop/st_2195/st2195_assignment_3/r_sql/q5_output.csv")

# Disconnect
dbDisconnect(conn)
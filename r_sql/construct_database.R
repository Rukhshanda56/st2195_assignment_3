library(DBI)
library(RSQLite)
library(readr)

# === SET YOUR DIRECTORY CONTAINING THE .bz2 AND .csv FILES ===
data_dir <- "~/Desktop/st_2195/st2195_assignment_3_database/dataverse_files"

# === CREATE DATABASE in same folder ===
db_path <- file.path(data_dir, "airline2.db")
con <- dbConnect(SQLite(), db_path)

# === Get .csv.bz2 files for 2000–2005 ===
files <- list.files(path = data_dir, pattern = "^200[6-8]\\.csv\\.bz2$", full.names = TRUE)

# === Read and append into "ontime" table ===
for (file in files) {
  message("Processing: ", file)
  df <- read_csv(file)
  dbWriteTable(con, "ontime", df, append = TRUE)
}

# === Read and write lookup tables ===
airports <- read_csv(file.path(data_dir, "airports.csv"))
dbWriteTable(con, "airports", airports, overwrite = TRUE)

carriers <- read_csv(file.path(data_dir, "carriers.csv"))
dbWriteTable(con, "carriers", carriers, overwrite = TRUE)

planes <- read_csv(file.path(data_dir, "plane-data.csv"))
dbWriteTable(con, "planes", planes, overwrite = TRUE)

dbDisconnect(con)
cat("Database successfully created at:", db_path, "\n")
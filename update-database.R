library(lubridate)
library(purrr)
library(jsonlite)
library(readr)
library(httr)
library(dplyr)

########
# Utilisation of the lubridate package
########

options(lubridate.week.start = 1)  # To start the week on day 1 (package parameter)
today <- today()

########
# Sensor list
########

# /!\ The order of the following lists is important, as it links sensor ids to their names /!\

sensor_ids <- c(9000002156, 9000001906, 9000001618,9000003090,9000002453,9000001844,
                9000001877,9000002666,9000002181,9000002707,9000003703,
                9000003746,9000003775,9000003736,9000004971,9000004130,
                9000004042,9000004697)

sensor_names <- c("Burel-01","Leclerc-02","ParisMarche-03","rueVignes-04","ParisArcEnCiel-05","RteVitre-06",
                  "RueGdDomaine-07","StDidierNord-08","rueVallee-09","StDidierSud-10","RuePrieure-11",
                  "RueCottage-12","RueVeronniere-13","RueDesEcoles-14","RueManoirs-15","RueToursCarree-16",
                  "PlaceHotelDeVille-17","BoulevardLiberté-18")


########
# API key (see the Telraam site to generate one)
########

if (file.exists('clef.txt')){
  key1 <- c(
    'X-Api-Key' = readLines("clef.txt")
  )
  key <- readLines("clef.txt")
} else {
  key <- NULL
}

# checking api state

api_state <- function(){
  VERB("GET", url = "https://telraam-api.net/v1", add_headers(key1))$status_code == 200  # the request suceeded if equal to 200
}


# Retrieve sensor data from the API

retrieve_sensor <- function(id_sensor,date1,date2){
  # Initialization
  result <- data.frame()
  date2 <- date2 + days(1) # so that date2 is included
  dates <- seq_by_3_month(date1,date2) # for the iteration of the retrieving, because when we call the API, the period can not exceed 3 month for each call
  
  # calling of the API
  resTraffic_list <- pmap(list(dates$start, dates$end), ~ {
    resTraffic <- POST("https://telraam-api.net/v1/reports/traffic",
                       add_headers("X-Api-Key" = key),
                       body = paste0('{
                       "level": "segments",
                       "format": "per-hour",
                       "id": "', id_sensor, '",
                       "time_start": "', ..1, '",
                       "time_end": "', ..2, '"
                     }'))
    # so that the data can be processed
    content <- resTraffic$content %>%
      rawToChar() %>%
      fromJSON()
    df <- content$report
    df$date <- ymd_hms(df$date, tz = df$timezone[1])
    df
  })
  
  # conversion from list to data.frame
  result <- bind_rows(resTraffic_list)
  
  if (length(result$date)!=0){ # in case the download is empty
    result$date <- ymd_hms(result$date, tz = result$timezone[1]) # We change the class of date with a time difference of 2
  }
  return(result)
}

seq_by_3_month <- function(date1, date2){
  if (date1==date2){
    return(data.frame(start = date1, end = date1))
  }else{
    date <- seq(from = date1, to = date2, by = "3 month")
    if (date[length(date)]!=date2){
      date <- c(date,date2)
    }
    return(data.frame(start = date[1:(length(date)-1)],
                      end   = date[2:length(date)]))
  }
}

# Write the database in the data folder for one sensor. This function handles both creation and update.

write_update_data <- function(id_sensor, date1, date2){
  # Preparation of the dataset
  data <- retrieve_sensor(id_sensor,date1, date2)
  # conversion from a numeric vector to a character string of car_speed_hist_0to70plus and car_speed_hist_0to120plus
  data$car_speed_hist_0to70plus <- sapply(data$car_speed_hist_0to70plus, function(x) paste(x, collapse = ", "))
  data$car_speed_hist_0to120plus <- sapply(data$car_speed_hist_0to120plus, function(x) paste(x, collapse = ", "))
  
  file_name <- paste0("data/",sensor_names[which(sensor_ids==id_sensor)],".csv")
  
  if (!is.null(data)){
    if (file.exists(file_name)){
      cleaning <- read_csv2(file_name)
      data <- rbind(cleaning,data)
      data <- data[!duplicated(data$date),] # if some lines are repeated they are eliminated
    }
    write_csv2(data, file = file_name)
  }
}

## Initialization of the update

date_filepath <- "data/date.txt"

# Function to check if an update is possible
checkUpdatePossible <- function(date) {
  return(as.Date(date) < as.Date(Sys.Date()))
}

# Function to update the database
updateDatabase <- function(update) {
  date <- as.Date(update$date)
  
  # Check if an update is possible, otherwise nothing happens
  if (checkUpdatePossible(date)) {
    # Check the existence of the API's key
    if (is.null(key)) {
      update$state <- "The API key is missing."
    } else if (!api_state()) {
      update$state <- "There seems to be a problem with the API. Please wait until tomorrow or contact support."
    } else {
      # Update the database
      for (id_sensor in sensor_ids) { # Iteration on all sensors
        yesterday <- Sys.Date() - 1 # Today is excluded
        write_update_data(id_sensor, date1 = date, date2 = yesterday)
      }
      
      # Update the date of the next update
      writeLines(as.character(Sys.Date()), con = date_filepath)
      update$date <- Sys.Date()
      
      # Update the state of the database
      update$state <- paste0("The data ranges from 2021-01-01 to ", Sys.Date() - 1, ". The database has been updated. Please restart the application.")
    }
  }
}

# Initialize the update object
if (file.exists(date_filepath)) { # When the database already exists
  date <- readLines(date_filepath)
  
  # Check if an update is possible
  if (checkUpdatePossible(date)) {
    action <- ", an update with the Telraam API is possible."
  } else {
    action <- ", the database is up to date with the Telraam API."
  }
  
  update <- list(
    state = paste0("The data stored in the application ranges from 2021-01-01 to ", date, action),
    date = date,
    key = NULL
  )
  
} else { # When the database is empty
  update <- list(
    state = "The database is empty, please update the data.",
    date = "2021-01-01", # The database begins on 2021-01-01
    key = NULL
  )
}

# Update the database
updateDatabase(update)
########
# Package import
########
library(shiny) # for the app
library(tidyverse) # tibble import
library(dplyr) # tibble manipulation
library(ggplot2) # chart
library(httr) # for the import
library(jsonlite) # for the import
library(lubridate) # date format management
library(cowplot) # graphic overlay
library(CPAT) # Darling Erdos test
library(synchrony) # peak function
library(forecast) # moving average function
library(zoo) # na.trim function
library(readr) # csv manipulation
library(mgcv) # gam function (lisser les courbes)

########
# File import
########
source('1-accueil.R')
source('2-import.R')
source('3-comparaison_periode.R')
source('4-heure_engorg.R')
source('5-seuil_engorg.R')
source('6-comparaison.R')



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


########
### Import public holiday and vacation dates (from a government API,this part must be re-adapted if there is a problem/change in the gouv rating)
########

### Public holiday (imported over 25 years: 285 days) ###

public_holidays <- GET("https://calendrier.api.gouv.fr/jours-feries/metropole.json") %>% 
                    .$content %>% 
                    rawToChar() %>% 
                    fromJSON() %>% 
                    names() %>% 
                    ymd()

### Import vacation date (specific to Chateaubourg, see location (academy))
vacations <- GET(url =  "https://data.education.gouv.fr/api/v2/catalog/datasets/fr-en-calendrier-scolaire/exports/json",
                 query = list(refine = "location:Rennes",
                             exclude = "population:Enseignants")) %>%
                 .$content %>%
                 rawToChar() %>%
                 fromJSON() %>%
                 select(description,
                        start_date,
                        end_date) %>%
                 mutate(
                   start_date = ymd_hms(start_date),
                   end_date = ymd_hms(end_date))


########
### Functions
########
# These functions are used throughout the whole application, that is why they are not in the others modules

########################
# Date selection function
########################


#' Filtering dataframe rows by date
#'
#' @param data a dataframe with a "date" column (lubridate format)
#' @param date_range Date or character. c("aaaa-mm-jj","aaaa-mm-jj")
#'
#' @return 
#' @export
filter_date <- function(data, date_range){
  # in case dates are exchanged
  date1 <- ymd(date_range[1])
  date2 <- ymd(date_range[2])
  if (date1<date2){
    start <- date1
    end <- date2
  } else {
    start <- date2
    end <- date1
  }
  # Filter according to dates
  matching_dates <- (data$date>=start & data$date<=end)
  return(data[matching_dates,])
}

########################
# Vacation selection function
########################


#' Filtering dataframe rows by date
#'
#' @param data a dataframe with a "date" column (lubridate format)
#' @param vacance character. "Oui" vacations included
#'                           "Non" vacations not included
#'                           "Seulement les vacances" vacations only
#'
#' @return 
#' @export
filter_vacation <- function(data, vacation){
  if (vacation=='Oui'){
    return(data)
  }  
  
  # Building of the filter
  matching_dates <- rep(FALSE,nrow(data))
  for (i in 1:nrow(vacations)){ 
    temp <- (data$date>=vacations$start_date[i] & data$date<=vacations$end_date[i])
    matching_dates <- matching_dates + temp
  }
  matching_dates <- (matching_dates>0)
  
  if(vacation=="Non"){
    return(data[!matching_dates,])
  }
  if(vacation=="Seulement les vacances"){
    return(data[matching_dates,])
  }
}

########################
# Public holidays selection function
########################


#' Filtering dataframe rows by date
#'
#' @param data a dataframe with a "date" column (lubridate format)
#' @param vacance character. "Oui" public holidays included
#'                           "Non" public holidays not included
#'                           "Seulement les vacances" public holidays only
#'
#' @return 
#' @export
filter_public_holidays <- function(data, JF){
  if (JF=='Oui'){
    return(data)
  }  
  d <- substr(as.character(data$date),1,10)
  matching_dates <- rep(0,nrow(data))
  chara <- as.character(public_holidays)
  for (ferie in chara){
    matching_dates <- matching_dates + (ferie==d)
  }
  matching_dates <- (matching_dates>0)
  
  if(JF=="Non"){
    return(data[!matching_dates,])
  } else if(JF=="Seulement les jours fériés"){
    return(data[matching_dates,])
  }
}


########################
# Filter function
########################


#' Filter by selected criteria. 
#' Not all criteria need to be filled in. Unfilled criteria are set by default so that no filtering is performed.
#'
#' @param data Raw data. See the "importation" function in the '2-import.R' file
#' @param sensor character. Name of desired sensor
#' @param direction character. Direction of the street: " " or "_lft" or "_rgt"
#' @param mobility character. Type of mobility: c("car","heavy","pedestrian","bike")
#' @param date_range character. Date or character. c("aaaa-mm-jj","aaaa-mm-jj")
#' @param vac character. With, without, or only with vacation: "Oui" or "Non" or "Seulement les vacances"
#' @param p_h character. With, without, or only with public holiday: "Oui" or "Non" or "Seulement les jours fériés"
#' @param wkd character. Selected days of the week: c("1","2","3","4","5","6","7") here all days are selected
#'
#' @return 
#' @export
filtering <- function(data = NULL, sensor    = NULL, direction = ' ', mobility  = c("car","heavy","pedestrian","bike"),
                     date_range = NULL, vac  = NULL, p_h = NULL, wkd  = NULL
){
  if (is_empty(data)|is.null(data)|is.null(sensor)|is.null(mobility)|is.null(direction)){
    return(tibble())
  }
  filtre <- data[ data$segment_id == sensor, ]
  S <- trimws(paste0(mobility,direction))
  filtre$total <- apply(filtre[,S], MARGIN = 1 ,FUN = sum)
  if (!is.null(wkd)){
    filtre <- filtre %>% filter(wday(date) %in% wkd)
  }
  if (!is.null(date_range)){
    filtre <- filtre %>% filter_date(date_range)
  }
  if (!is.null(vac)){
    filtre <- filtre %>% filter_vacation(vac)
  }
  if (!is.null(p_h)){
    filtre <- filtre %>% filter_public_holidays(p_h)
  }
  return(filtre)
}

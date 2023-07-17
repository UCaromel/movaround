
########################
# State of the API
########################


#'  Return the state of he Telraam API. Determine if updates can be made.
#'
#' @param 
#'
#' @return TRUE if the API responds well, FALSE otherwise
#' @export
api_state <- function(){
  VERB("GET", url = "https://telraam-api.net/v1", add_headers(key1))$status_code == 200  # the request suceeded if equal to 200
}

########################
# Simple plot
########################


#'  Number of cars and HGVs as a function of time
#'
#' @param data data.frame. See importation function
#' @param sensor character. Name of the chosen sensor
#' @param date_range Date vector. example: c('2021-01-01','2022-01-01')
#'
#' @return Simple chart
#' @export
#' @examples
simple_plot <- function(data, sensor, date_range){
  data <- filtering(data, sensor = sensor,  date_range = date_range, mobility = c('car','heavy'))
  
  # the returned graphic
  ggplot(data)+
    aes(x=date, y=total) +
    geom_line(color="black")+
    geom_smooth(method='gam', formula=y ~ s(x, bs = "cs"),color="#B1D62E", size=2) + # trend
    ylab("Nombre de voitures et de poids lourds")+
    xlab("Date")+
    ggtitle("Nombre de voitures et de poids lourd en fonction du temps")+ # legend
    theme_bw() + # set the design of the chart
    theme(panel.background = element_rect(fill = "#F5F5F5"), # background color
          panel.grid = element_line(color = "#E3E3E3"), # grid color
          panel.border = element_rect(color = "#E3E3E3", size=2)) # border color and size
}

########################
# Retrieve the data associated to a sensor
########################


#'  Retrieve the data associated to a sensor from the API Telraam
#'
#' @param id_sensor Numeric. ID of the sensor
#' @param date1 Date. Start date "aaaa-mm-jj"
#' @param date2 Date. End date "aaaa-mm-jj"
#'
#' @return Data from the API, from date1 to date2. Date1 and date2 are included.
#' @export
#' @examples
#' retrieve_sensor(9000002156, ymd('2021-05-21'),ymd('2021-05-30'))
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


########################
# Write the database of one sensor: creation/update
########################


#'  Write the database in the data folder one sensor. This function does both creation and update.
#'
#' @param nom id_sensor Numeric. ID of the sensor
#' @param date1 Date. Start date "aaaa-mm-jj"
#' @param date2 Date. End date "aaaa-mm-jj"
#'
#' @return
#' @export
#' @examples
#' write_update_data(9000002156, ymd('2021-05-21'),ymd('2021-05-30'))
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

########################
# Sequence generation that cut a period by 3 months
########################


#' Sequence generation that cut a period by 3 months. Only used in the function retrieve_sensor.

#'
#' @param date1 Date. Start date "aaaa-mm-jj"
#' @param date2 Date. End date "aaaa-mm-jj"
#'
#' @return A dataframe with two columns. One row correspond to one interval of three months.
#' @export
#' @examples
#' seq_by_3_month(ymd('2020-05-21'),ymd('2021-05-30'))
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

########################
# Convert a numeric vector into a character string
########################


#' Convert a character string into a numeric vector
#' @param vector Something in the shape "10,20,30"
#'
#' @return Numeric vector. Something in the shape c(10,20,30)
#' @export
#' @examples
#' convert_string_to_list("10,20,30")
convert_string_to_list <- function(vector){
  convert <- as.list(vector)
  lapply(vector, function(x) {
    elements <- unlist(strsplit(x, ",\\s*"))
    numeric_elements <- as.numeric(elements)
    return(numeric_elements)
  })
}

########################
# Importation of data associated to a list of sensor
########################


#' Importation of data associated to a list of sensor
#' @param Numeric. A list of sensor's ID
#'
#' @return data.frame
#' @export
#' @examples
#' importation(c(9000002156,9000001906))
importation <- function(list_sensor){
  data <- data.frame()
  name_sensor <- sensor_names[sensor_ids%in%list_sensor]
  data <- map_dfr(name_sensor, ~ {
    file <- paste0('data/', .x, '.csv')
    if (file.exists(file)) {
      # we select the data that we don't consider null (arbitrary choice)
      import <- read_csv2(file) %>% filter(uptime > 0.5,
                                  heavy_lft + car_lft + pedestrian_lft + bike_lft +
                                    heavy_rgt + car_rgt + pedestrian_rgt + bike_rgt >0)
      import$car_speed_hist_0to70plus <-  convert_string_to_list(import$car_speed_hist_0to70plus)
      import$car_speed_hist_0to120plus <- convert_string_to_list(import$car_speed_hist_0to120plus)
      import$date <- ymd_hms(import$date)
      

      import
    } else {
      NULL
    }
  })
  data
}


##############################################
#                  Module                    #
##############################################
ui_2 <- function(id){
  ns <- NS(id)
  tagList( # allows the module to return several objects instead of just one
    ######## css parameter to adjust image size ########
    tags$head(
      tags$style(HTML("
        #full-width-image {
        width: 100%;
        }
      "))
    ),
    
    ######### display #########
    tags$img(id = "full-width-image", src = "images/chateaubourg2.png"),
    br(), # return to line
    br(),
    h2("Les données"),
    br(),
    ########
    # Sensors choices part
    ########
    h3("Choix des capteurs"),
        p(class="text-center",
      "Afin de pouvoir utiliser les outils d'analyse fournis par cette application, veuillez cocher les capteurs qui vous intéressent."),
    br(),
    fluidRow(
          column(2),
          column(4,
                 br(),
                 checkboxGroupInput(
                   inputId=ns("Capteurs"),
                   label="Liste des capteurs disponibles :",
                   choiceNames = sensor_names,
                   choiceValues = sensor_ids
                 ),
                 textOutput(ns("import_state"))),            
          column(3, class = "text-center",
                 h4('Carte des capteurs de Chateaubourg'),
                 p("Un numéro correspond à un capteur."),
                 tags$img(src="images/carte capteur apli.jpg",height=400),
          column(3)
                 
          )
    ),
    
    br(),
    br(),
    
    ########
    # Simple chart part
    ########
    uiOutput(ns('display_simple_plot')),
    br(),
    
    ########
    # Update part
    ########
    h3("Mise à jour des données"),
    div(class = "text-center",
        p("Les données qu'utilisent cette application proviennent de l'API Telraam."),
        textOutput(ns("update_state")), br(),
        actionButton(ns("update"), "Mettre à jour les données")
    ),
    textOutput(ns("existence_key")),
    br(),
    
    ########
    # Missing data part
    ########
    h3(class="text-center","Les valeurs manquantes"),
    p(class="text-center",'à compléter'),
    h4("Avertissement relatif à la qualité des données :"),
    p(class="text-center","Les données des capteurs Telraam ne sont pas issues d’une mesure continue sur une heure. Pour améliorer la qualité des données futures,
    les capteurs dédient une partie de leur temps d’activité à l’apprentissage. Les données totales sont reconstituées à partir du temps de mesure.
    Plus cette période de mesure est longue plus la qualité des données est grande. Telraam donne un outil de mesure de ce temps de mesure : l’uptime.
    Dans cette application, nous n'avons conservé que les données d’uptime supérieur à 0.5 (seuil conseillé par Telraam). 
    Toutefois, les capteurs placés récemment (en période d’apprentissage) et les données matinales ou dans la soirée (visibilité réduite à cause de la nuit)  peuvent présenter des uptimes plus faible. 
    De plus la suppression des données à l’uptime trop faible fait qu’on possède moins de données pour les périodes à risque. La qualité des estimations
    et des tests est moins bonne sur ces périodes. Il faut donc être prudent en interprétant ces données."),
    br(),
    
    ########
    # Warning part
    ########
    h3("Avertissement relatif aux catégories de mobilités :"),
    p(class = "text-center","Les capteurs Telraam utilisés ont des difficultés à différencier les grosses voitures comme les SUV des poids lourds. Le nombre de poids lourds est donc sur-évalué et le nombre de voitures sous-évalué. Toutefois, le total voitures + camions est précis.
    De la même façon, il faut être prudent dans la différenciation entre vélos et piétons."),
    br(),
    p(class = "text-center",
      "Pour plus d'informations, veuillez consulter",
      tags$a(href="https://telraam.net/#14/48.1014/-1.3862","le site Telraam"),"."),    
            
  )
}


server_2 <- function(input, output, session){
  ns <- session$ns
  
  #################################
  #          Importation          #
  #################################
  # Initialization of the reactive
  data <- reactiveValues(
    sensors = NULL,
    data = tibble()
  )
  
  
  # Update of data reactive
  observe({
    if (!file.exists('data/date.txt')){
      output$import_state <- renderText({"La base de données est vide, veuillez cliquer sur le bouton mise à jour"})
      
    } else if (is.null(input$Capteurs)){
      output$import_state <- renderText({"Pas de capteurs sélectionnés"})
      
    } else {
      # if truc pas NULL alors keep
      add <- setdiff(input$Capteurs, data$sensors) %>% importation()
      data$data <- data$data %>% rbind(add) %>% # add new sensors  
                                 filter((segment_id %in% input$Capteurs)) # remove sensors that are not selected anymore
      output$import_state <- renderText(paste("Les capteurs importés sont: ", paste(sensor_names[sensor_ids%in%input$Capteurs],collapse=', ')))
      data$sensors <- input$Capteurs
    }
  })
  
  
  
  # For downloading the imported data
  output$downloadData <- downloadHandler(
    filename = "Donnee_brute.csv",
    content = function(file) {
      write_excel_csv2(data$data, file)
    }
  )
  
  
  #################################
  #            Update             #
  #################################
  
  ## Initialization of the update reactive 
  
  if (file.exists("data/date.txt")){ # When the database already exists
    
    date <- readLines("data/date.txt")
    
    # to see if an update is possible
    if (ymd(date) <  today){
      action <- ", une mise à jour avec l'API Telraam est possible."
    } else {
      action <- ", la base de données est à jour avec l'API Telraam."
    }
    
    update <- reactiveValues(
      state = paste0("Les données stockées dans l'application vont du 2021-01-01 au ", date, action) ,
      date = date,
      key = NULL
    )
    
  } else { # When the database is empty
    update <- reactiveValues(
      state = "La base de données est vide, veuillez mettre à jour les données",
      date = "2021-01-01", # the database begins the 2021-01-01
      key = NULL
    )
  }
  
  # Update of the database reactif after triger of the update button
  observeEvent(input$update,{
    date <- ymd(update$date)
    
    # determination of if an update is possible, otherwise nothing happened
    if (date < today){
      
      # verification of the existence of the API's key
      if (is.null(key)){
        update$state <- "La cle pour appeler l'API n'existe pas"
      } else if (!api_state()){
        update$state <- "Il semble y avoir un problème au niveau de l'API. Veuillez attendre le lendemain ou contacter le support."
      } else {
        # the update is launched
        withProgress(message = "Mise à jour de", value = 0,{ # progress bar
          incProgress(0, detail = paste("Capteurs"))
          k <- 2
          for (id_sensor in sensor_ids) { # iteration on all sensors
            incProgress(1/(length(sensor_ids)+1), detail = sensor_names[(sensor_ids==id_sensor)]) # incrementation of the progress bar
            k <- k+1
            yesterday <- today - days(1) # ! today is excluded !
            write_update_data(id_sensor, date1 = date, date2 = yesterday) 
          }
        })
        
        # update of the date of the next update
        writeLines(as.character(today), con = "data/date.txt")
        update$date <- today
        
        # update of the state of the database
        update$state <- paste0("Les données vont du 2021-01-01 au ", today - days(1),", la base de donnée a été mise à jour, veuillez redémarrer l'application")
      }
    }
  })
  
  output$update_state <- renderText({
    update$state
  })
  
  output$existence_key <- renderText({
    update$key
  })
  
  #################################
  #            Plot               #
  #################################
  
  observe({ # update sensor selection according to import tab
    if (!is.null(data$sensors)){
      names_selected_sensors <- setNames(data$sensors,sensor_names[sensor_ids%in%data$sensors])
      updateSelectInput(session, "sensor_plot", choices = names_selected_sensors)
    }
  })
  
  output$display_simple_plot <- renderUI({
    if (!is.null(data$sensors)){
      fluidRow(
        h3("Les capteurs sélectionnés"),
        column(3),
        column(3,
               selectInput(ns("sensor_plot"),
                           label = "Choix du capteur à tracer",
                           choices = NULL)
        ),
        column(3,
               dateRangeInput(ns("date_range_simple_plot"), "Période",
                              start  = "2021-01-01",
                              end    = Sys.Date() - days(1),
                              min    = "2021-01-01",
                              max    = Sys.Date() - days(1))
        ),
        column(3),
        plotOutput(ns('simple_plot')),
        br(),br(),br(),br(),
        column(2 , offset = 10,
               downloadButton(ns("downloadData"), "Import des données")
        )
      )
    }
  })
  
  output$simple_plot <- renderPlot({
    simple_plot(data$data,sensor=input$sensor_plot,date_range = input$date_range_simple_plot)
  })
  
  
  
  # Return value of the module
  return(data)
}

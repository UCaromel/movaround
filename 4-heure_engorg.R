plot_hour_threshold <- function(...) {
  data <- do.call(filtering,list(...))

  if (is_empty(data)) {
    return(NULL)
  }

  data <- data %>%
    group_by(hour = hour(date)) %>%
    summarise(v85 = mean(v85, na.rm = TRUE),
              B_to_A = mean(car_rgt + heavy_rgt, na.rm = TRUE),
              A_to_B = mean(car_lft + heavy_lft, na.rm = TRUE))


  # Calculate means and standard deviations if both directions are present

    mean_voiture <- mean(c(data$B_to_A, data$A_to_B))
    sd_voiture <- sd(c(data$B_to_A, data$A_to_B))
    mean_speed <- mean(data$v85)
    sd_speed <- sd(data$v85)

    data <- data %>%
      mutate(v85 = ((v85 - mean_speed) / sd_speed) * sd_voiture + mean_voiture) %>%
      pivot_longer(cols = c(v85, B_to_A, A_to_B), names_to = "Legend", values_to = "valeur")


  graph <- ggplot(data, aes(x = hour, y = valeur, group=Legend, shape=Legend, colour=Legend)) +
           geom_line(aes(linetype=Legend),size = 1) +
           geom_point(size = 3)+
           scale_color_manual(values = c("#006bb6", "#006bb6", "#ff5900"))+ # legend
           scale_linetype_manual(values = c("dotted", "solid","solid")) + # legend
           ylab('Nombre de véhicules moyen')+
           scale_y_continuous(sec.axis = sec_axis(~((.-mean_voiture)/sd_voiture)*sd_speed+mean_speed, # tracing the second axis
                                                  name = "Vitesse v85 moyenne (km/h)")) +
           scale_x_continuous(breaks = min(data$hour):max(data$hour)) +
           theme_bw() +
           theme(legend.position = "bottom", legend.box = "horizontal", # the whole code in this theme() function is about color
                 axis.ticks.y.left = element_line(color = "#006bb6"), # ticks (left axis)
                 axis.text.y.left = element_text(color = "#006bb6"), # number associated to a tick (left axis)
                 axis.title.y.left = element_text(color = "#006bb6"), # title of an axis (left axis)
                 axis.title.y.right = element_text(color = "#ff5900"), # ticks (right axis)
                 axis.text.y.right = element_text(color = "#ff5900") , # number associated to a tick (right axis)
                 axis.ticks.y.right = element_line(color = "#ff5900"), # title of an axis (right axis)
                 panel.background = element_rect(fill = "#F5F5F5"), # background
                 panel.grid = element_line(color = "#E3E3E3"), # grid
                 panel.border = element_rect(color = "#E3E3E3", size = 2)) #border of the chart

  return(list(graph=graph,data=data))

}



##############################################
#                  Module                    #
##############################################
ui_4 <- function(id){
  ns <- NS(id)
  tagList(
    column(3,wellPanel(
      selectInput(ns("sensor"),
                  label = "Choix du capteur",
                  choices = NULL),
      dateRangeInput(ns("date_range"), "Période",
                     start  = "2021-01-01",
                     end    = Sys.Date()-days(1),
                     min    = "2021-01-01",
                     max    = Sys.Date()-days(1)),
      radioButtons(inputId = ns("vacation"), label = "Vacances comprises :",
                   choices = c("Oui","Non","Seulement les vacances"),selected = "Non"),
      radioButtons(inputId = ns("p_h"), label = "Jours fériés compris :",
                   choices = c("Oui","Non","Seulement les jours fériés"),selected = "Non"),
      checkboxGroupInput(
        ns("wkd"),
        "Choix des jours",
        selected = 1:5,
        choiceNames = c("lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"),
        choiceValues = 1:7,
        inline = TRUE
      )
    )),

    h3("Heure d'engorgement :"),
    p("Cet onglet permet de visualiser, pour un capteur, la circulation dans chaque direction
                 (voitures et poids lourds) et la vitesse V85 (vitesse telle que 85 % des usagers roulent
                 plus lentement), en fonction des heures de la journée. On peut alors observer à quelle heure aparaissent les ralentissements."),
    br(),
    uiOutput(ns("display"))
  )
}

server_4 <- function(input, output, session, data){
  ns <- session$ns

  observe({  # update sensor selection according to import tab
    if (!is.null(data$sensors)){
      names_selected_sensors <- setNames(data$sensors,sensor_names[sensor_ids%in%data$sensors])
      updateSelectInput(session, "sensor", choices = names_selected_sensors)
    }
  })

  #--- function application ---
  result <- reactive({
    plot_hour_threshold(data = data$data, sensor = input$sensor, date_range = input$date_range, vac = input$vacation, p_h = input$p_h, wkd = input$wkd)
  })

  #--- output definition ---
  output$plot_hour_threshold <- renderPlot({
    result()$graph
  })

  output$display <- renderUI(
    if (is.null(data$sensors)){
      p(class="text-center", "Pour afficher le graphique, veuillez sélectionner des capteurs dans l'onglet import.")
    }
    else if (is.null(input$wkd)){
     p("Le graphique est vide pour les critères sélectionnés")
    } else {
      column(width = 9,
             h3("Quelle est l'heure d'engorgement ?"),
             plotOutput(ns("plot_hour_threshold")),
             p("La vitesse V85 est la vitesse telle que 85% des usagers roulent plus lentement que celle ci."),
             downloadButton(ns("downloadHeure"), "Import des données")
      )
    }
  )

  output$downloadHeure <- downloadHandler(
      filename = "Heure_engorgement.csv", # Nom du fichier importé
      content = function(file) {
        write_excel_csv2(result()$data, file)
      }
    )
}

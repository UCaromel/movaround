shinyUI(
    navbarPage(
      title = "Mov-around",
      theme = "style/style.css",
      fluid = TRUE, 
      collapsible = TRUE,
      tabPanel("Accueil", ui_1("1")),
      tabPanel("Les données", ui_2("2")),
      tabPanel("Comparaison de périodes", ui_3("3")),
      tabPanel("Heure d'engorgement", ui_4("4")),
      tabPanel("Seuil d'engorgement", ui_5("5")),
      tabPanel("Comparaison de deux capteurs", ui_6("6"))
    )
)

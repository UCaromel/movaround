ui_1 <- function(id){
  tagList(
    ### parameter to adjust image size
    tags$head(
      tags$style(HTML("
        #full-width-image {
          width: 100%;
        }
       "))
    ),
    ### display
    tags$img(id = "full-width-image", src = "images/chateaubourg2.png"),
    br(), # return to line
    br(),
    br(),
    h1('Bienvenue'),
    p(class = "text-center",
    "Cette application a pour objectif de fournir des outils d’analyse du trafic routier de la commune de Châteaubourg."),
    p(class = "text-center",
    "Des capteurs Telraam ont été installés dans certaines rues de Châteaubourg: Chaque capteur compte le nombre de voitures, de poids lourds, de vélos ainsi que de piétons qui circulent."),
    p(class = "text-center","Pour commencer à utiliser cette application, cliquer sur l'onglet 'Les données'"),
    br(),
    br(),
    br(),
    tags$img(id = "full-width-image", src = "images/chateaubourg.png")
  )
}

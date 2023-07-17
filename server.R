shinyServer(function(session,input, output) {
  data <- callModule(server_2,id = "2",session = session)
  callModule(server_3, id = "3", session = session, data)
  callModule(server_4, id = "4", session = session, data)
  callModule(server_5, id = "5", session = session, data)
  callModule(server_6, id = "6", session = session, data)
})

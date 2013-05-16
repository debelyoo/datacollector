package ecol

import spray.can.server.SprayCanHttpServerApp
import akka.actor.Props


object Boot extends App with SprayCanHttpServerApp {

  // create and start our service actor
  val apiService = system.actorOf(Props[ApiServiceActor], "apiService")

  // create a new HttpServer using our handler tell it where to bind to
  newHttpServer(apiService) ! Bind(interface = "localhost", port = 7070)
}
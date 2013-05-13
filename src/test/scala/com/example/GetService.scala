package ecol

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import spray.json._

class GetServiceActor extends Actor with GetService {

  def actorRefFactory = context

  def receive = runRoute(getRoute)
}

trait GetService extends HttpService {

  val getRoute =
    path("") {
      get {
        respondWithMediaType(`application/json`) {
          complete {
            val source = "{ \"param\": \"JSON source\" }"
            source.asJson
          }
        }
      }
    }

}
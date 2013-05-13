package ecol

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import spray.json._
import MySprayJsonSupport._



// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class ApiServiceActor extends Actor with PutService with GetService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(putRoute ~ getRoute)
}


// this trait defines our service behavior independently from the service actor
trait PutService extends HttpService {
  //import DefaultJsonProtocol._

  val putRoute =
    path("") {
      put {
        respondWithMediaType(`application/json`) {
          complete {
            /*<html>
              <body>
                <h1>You PUT something!</h1>
              </body>
            </html>*/
            val source = "{ \"status\": \"PUT successful\" }"
            source.asJson.asJsObject
          }
        }
      }
    }

}

trait GetService extends HttpService {

  val getRoute =
    path("") {
      get {
        respondWithMediaType(`application/json`) {
          complete {
            val source = "{ \"status\": \"GET successful\" }"
            source.asJson.asJsObject            
          }
        }
      }
    }

}
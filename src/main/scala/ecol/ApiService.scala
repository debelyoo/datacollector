package ecol

import akka.actor.Actor
import spray.routing._
import spray.http._
import spray.http.MediaTypes._
import spray.json._
import MySprayJsonSupport._
import java.util.Date
import java.text.SimpleDateFormat
import ecol.cassandra.AstyanaxConnector
import ecol.cassandra.model.TemperatureLog
import ecol.test.DataPusher
import ecol.util.Message
import akka.actor.actorRef2Scala
import spray.routing.Directive.pimpApply
import spray.routing.directives.CompletionMagnet.fromObject



// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class ApiServiceActor extends Actor with PutService with GetService with PostService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  AstyanaxConnector.init() // initialize the DB connector
  
  def receive = runRoute(getRoute ~ postRoute)
}
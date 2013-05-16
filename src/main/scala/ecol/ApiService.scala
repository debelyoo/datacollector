package ecol

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import spray.json._
import MySprayJsonSupport._
import java.util.Date
import java.text.SimpleDateFormat
import ecol.cassandra.AstyanaxConnector
import ecol.cassandra.TemperatureLogJsonProtocol._



// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class ApiServiceActor extends Actor with PutService with GetService with PostService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  AstyanaxConnector.init() // initialize the DB connector
  
  def receive = runRoute(getRoute ~ postRoute ~ putRoute)
}

trait PostService extends HttpService {

  val postRoute =
    pathPrefix("api" / "1.0") {
	  respondWithMediaType(`application/json`) {
	    post {
		  path("") {
			entity(as[JsObject]) { data =>
				println(data)
				complete {
				  AstyanaxConnector.insertTemperature("20.11")
				  val source = "{ \"status\": \"POST successful\" }"
				  source.asJson.asJsObject
				}
			}
		  }
	    }
	  }
  	}

}

trait PutService extends HttpService {

  val putRoute =
    pathPrefix("api" / "1.0") {
	  respondWithMediaType(`application/json`) {
	    put {
	    	path("") {
			  entity(as[JsObject]) { data =>
			    println(data)
			    complete {
			    	val source = "{ \"status\": \"PUT successful\" }"
			    	source.asJson.asJsObject
			    }
	          }
	        }
	      }
	   }
  	}

}

trait GetService extends HttpService {

  val getRoute =
    pathPrefix("api" / "1.0") {
	  respondWithMediaType(`application/json`) {
	    get {
	    	path("") {
	    		complete {
	    			val temps = AstyanaxConnector.getTemperatureByTimeRange
	    			val jArr = JsArray(temps.map(_.toJson))
	    			val jsonData = "{ \"items\": "+ jArr.toString +" }"
					jsonData.asJson.asJsObject            
				  }
			  }
		  }
	  	}
    }

}
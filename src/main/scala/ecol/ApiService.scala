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
import ecol.test.DataPusher
import ecol.cassandra.Message



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
				  AstyanaxConnector.insertWorker ! Message.InsertTemperature(new Date(), "address1", "20.11")
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
	    	pathPrefix("temperature") {
	    	  pathPrefix("from" / PathElement) { startTime =>
	    	    path("to" / PathElement) { endTime => 
	    		  complete {
	    		    try {
	    		      val date1 = new SimpleDateFormat("yyyyMMdd-HHmmss").parse(startTime)
	    		      val date2 = new SimpleDateFormat("yyyyMMdd-HHmmss").parse(endTime)
	    		      val temps = AstyanaxConnector.getTemperatureByTimeRangeAndSensor(
	    		        timeRange = Some((date1, date2)),
	    		        sensorAddresses = Some(Seq("address1", "address3"))
	    		      )
	    		      val jArr = JsArray(temps.map(_.toJson))
	    		      val jsonData = "{ \"items\": "+ jArr.toString +", \"count\": \""+ temps.length +"\" }"
				      jsonData.asJson.asJsObject
	    		    } catch {
	    		      case ex: Exception => {
	    		        val source = "{ \"error\": \"parameters are badly formatted\" }"
	    		        source.asJson.asJsObject            
	    		      }
	    		    }
	    		  }
				}
	    	  }
			} ~
			path("temp") {
	    		complete {
	    		  val temps = AstyanaxConnector.getTemperature
	    		  println(temps)
	    		  val source = "{ \"status\": \"get returned !\" }"
			      source.asJson.asJsObject            
				}
			} ~
	    	path("test") {
	    		complete {
	    		  val dp = new DataPusher()
	    		  dp.pushSampleData
				  val source = "{ \"status\": \"test terminated\" }"
			      source.asJson.asJsObject
				}
			}
		}
	 }
   }

}
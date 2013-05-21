package ecol
import spray.routing._
import spray.http._
import spray.http.MediaTypes._
import spray.json._
import MySprayJsonSupport._
import ecol.cassandra.model.TemperatureLogJsonProtocol._
import ecol.cassandra.AstyanaxConnector
import ecol.util.Message
import ecol.util.DateFormatHelper._
import java.util.Date
import ecol.cassandra.model.TemperatureLog

trait PostService extends HttpService {

  val postRoute =
    pathPrefix("api" / "1.0") {
	  respondWithMediaType(`application/json`) {
	    post {
		  path("temperature") {
			entity(as[JsObject]) { data =>
				println(data)
				complete {
				  //try {
				    val tl = data.convertTo[TemperatureLog]
				    val date = dateTimeFormatter.parse(tl.ts)
				    AstyanaxConnector.insertWorker ! Message.InsertTemperature(date, tl.sensorAddress, tl.temperature)
				    val resp = "{ \"status\": \"POST successful\" }"
				    resp.asJson.asJsObject
				  /*} catch {
				    case ex: Exception => {
				      ex.printStackTrace()
				      val resp = "{ \"status\": \"POST failed\" }"
				      resp.asJson.asJsObject
				    }
				  }*/
				}
			}
		  }
	    }
	  }
  	}

}
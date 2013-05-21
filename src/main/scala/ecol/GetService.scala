package ecol

import spray.routing.HttpService
import spray.http.MediaTypes._
import spray.json._
import MySprayJsonSupport._
import spray.json.JsObject
import ecol.util.DateFormatHelper._
import ecol.cassandra.model.TemperatureLog
import ecol.cassandra.model.TemperatureLogJsonProtocol._
import ecol.test.DataPusher

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
	    		      val date1 = dateTimeFormatter.parse(startTime)
	    		      val date2 = dateTimeFormatter.parse(endTime)
	    		      val temps = TemperatureLog.getTemperatureByTimeRangeAndSensor(
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
	    		  val temps = TemperatureLog.getTemperature
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
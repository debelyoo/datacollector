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
import spray.routing.MalformedRequestContentRejection
import ecol.util.DateFormatHelper._
import java.util.Date

trait GetService extends HttpService {

  val getRoute =
    pathPrefix("api" / "1.0") {
	  respondWithMediaType(`application/json`) {
	    get {
	    	pathPrefix("temperature") {
	    	  pathPrefix("from" / PathElement) { startTime =>
	    	    pathPrefix("to" / PathElement) { endTime => 
	    	      path("forAddress" / PathElement) { addresses => 
	    		    try {
	    		      val optAddr = if (addresses == "all") None else Some(addresses)
	    		      val temps = requestTemperature(startTime, endTime, optAddr)
	    		      assert(temps.isDefined)
	    		      val jArr = JsArray(temps.get.map(_.toJson))
	    		      complete {
	    		        val jsonData = "{ \"items\": "+ jArr.toString +", \"count\": \""+ temps.get.length +"\" }"
				        jsonData.asJson.asJsObject
	    		      }
	    		    } catch {
	    		      case ae: AssertionError => {
	    		        reject {
	    		    	  MalformedRequestContentRejection("bad timestamp formatting")
	    		    	}
	    		      }
	    		      case pe: java.text.ParseException => {
	    		    	reject {
	    		    	  MalformedRequestContentRejection("bad timestamp formatting")
	    		    	}
	    		      }
	    		      case ex: Exception => {
	    		        ex.printStackTrace()
	    		        reject {
	    		          MalformedRequestContentRejection("unknown error")
	    		        }            
	    		      }
	    		    }
	    	      }
				}
	    	  }
			} ~
			/*path("temp") {
	    		complete {
	    		  val temps = TemperatureLog.getTemperature
	    		  println(temps)
	    		  val source = "{ \"status\": \"get returned !\" }"
			      source.asJson.asJsObject            
				}
			} ~*/
	    	path("ping") {
	    		complete {
				  val source = "{ \"status\": \"server is UP !\" }"
			      source.asJson.asJsObject
				}
			} ~ 
			path("test") {
	    		complete {
	    		  //val dp = new DataPusher()
	    		  //dp.pushSampleData
	    		  val tsRef = 3451386804.00000000 // nb of seconds from 1.1.1904
	    		  val valueDate = labViewTsToJavaDate(tsRef)
	    		  println(valueDate)
				  val source = "{ \"status\": \"test terminated\" }"
			      source.asJson.asJsObject
				}
			}
		}
	 }
   }
  
  /**
   * Request temperature logs
   * @param startTime The start time of the query window
   * @param endTime The end time of the query window
   * @param addresses The addresses to get (dash separated: addr1-addr2)
   * @return A list with the temperature logs (in an option)
   */
  def requestTemperature(startTime: String, endTime: String, addresses: Option[String]): Option[List[TemperatureLog]] = {
    if (startTime.length() != 15 || endTime.length() != 15)
      None
    else {
      val date1 = dateTimeFormatter.parse(startTime)
	  val date2 = dateTimeFormatter.parse(endTime)
	  val addr = addresses.map(_.split("-").toSeq)
	  val temps = TemperatureLog.getTemperatureByTimeRangeAndSensor(
	    timeRange = Some((date1, date2)),
	    sensorAddresses = addr
	  )
	  Some(temps)
    }
  }

}
package ecol

import spray.routing.HttpService
import spray.http.MediaTypes._
import spray.json._
import MySprayJsonSupport._
import spray.json.JsObject

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
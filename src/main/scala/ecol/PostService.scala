package ecol
import spray.routing._
import spray.http._
import MediaTypes._
import StatusCodes._
import spray.json._
import MySprayJsonSupport._
import ecol.cassandra.model.TemperatureLogJsonProtocol._
import ecol.cassandra.model.CompassLogJsonProtocol._
import ecol.cassandra.AstyanaxConnector
import ecol.util.Message
import ecol.util.DateFormatHelper._
import ecol.cassandra.model.{CompassLog, TemperatureLog}

trait PostService extends HttpService {

  val postRoute =
    pathPrefix("api" / "1.0") {
      respondWithMediaType(`application/json`) {
        post {
          path("temperature") {
            entity(as[JsObject]) { data =>
              println(data)
              try {
                val tl = data.convertTo[TemperatureLog]
                val date = dateTimeFormatter.parse(tl.ts)
                //val date = labViewTsToJavaDate(tl.ts.toDouble) // if TS comes from labview
                AstyanaxConnector.insertWorker ! Message.InsertTemperatureLog(date, tl.sensor, tl.value)
                complete {
                  val resp = "{ \"status\": \"POST successful\" }"
                  resp.asJson.asJsObject
                }
              } catch {
                case de: spray.json.DeserializationException => {
                  reject {
                    MalformedRequestContentRejection("bad JSON formatting")
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
          } ~
          path("compass") {
            entity(as[JsObject]) { data =>
              println(data)
              try {
                val cl = data.convertTo[CompassLog]
                val date = dateTimeFormatter.parse(cl.ts)
                //val date = labViewTsToJavaDate(tl.ts.toDouble) // if TS comes from labview
                AstyanaxConnector.insertWorker ! Message.InsertCompassLog(date, cl.sensor, cl.value)
                complete {
                  val resp = "{ \"status\": \"POST successful\" }"
                  resp.asJson.asJsObject
                }
              } catch {
                case de: spray.json.DeserializationException => {
                  reject {
                    MalformedRequestContentRejection("bad JSON formatting")
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
      }
  	}
}

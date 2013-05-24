package ecol

import spray.routing.HttpService
import spray.http.MediaTypes._
import spray.json._
import MySprayJsonSupport._
import ecol.cassandra.model.{CompassLog, TemperatureLog}
import ecol.test.DataPusher
import spray.routing.MalformedRequestContentRejection
import ecol.util.DateFormatHelper._
import ecol.cassandra.model.TemperatureLogJsonProtocol._
import ecol.cassandra.model.CompassLogJsonProtocol._

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
                  val temps = TemperatureLog.requestTemperatures(startTime, endTime, optAddr)
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
          pathPrefix("compass") {
            pathPrefix("from" / PathElement) { startTime =>
              path("to" / PathElement) { endTime =>
                try {
                  val vals = CompassLog.requestCompassValues(startTime, endTime)
                  assert(vals.isDefined)
                  val jArr = JsArray(vals.get.map(_.toJson))
                  complete {
                    val jsonData = "{ \"items\": "+ jArr.toString +", \"count\": \""+ vals.get.length +"\" }"
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

}
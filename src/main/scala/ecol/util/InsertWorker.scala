package ecol.util

import akka.actor.Actor
import ecol.cassandra.model.{CompassLog, TemperatureLog}

class InsertWorker extends Actor {

  def receive = {
    case Message.InsertTemperatureLog(ts, sensor, temperatureValue) => {
      //println("[RCV message] - insert temperature log: "+temperatureValue+", "+ sensor)
      TemperatureLog.insertValue(ts, sensor, temperatureValue)
    }
    case Message.InsertCompassLog(ts, sensor, compassValue) => {
      //println("[RCV message] - insert compass log: "+compassValue+", "+ sensor)
      CompassLog.insertValue(ts, sensor, compassValue)
    }
  }
}
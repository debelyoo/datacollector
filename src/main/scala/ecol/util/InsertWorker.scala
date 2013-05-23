package ecol.util

import akka.actor.Actor
import ecol.cassandra.model.TemperatureLog

class InsertWorker extends Actor {

  def receive = {
    case Message.InsertTemperature(ts, sensor, temperatureValue) => {
      //println("[RCV message] - insert temperature: "+temperatureValue+", "+ sensorAddress)
      //sensor.insertInDatabase
      TemperatureLog.insertTemperature(ts, sensor, temperatureValue)
    }
  }
}
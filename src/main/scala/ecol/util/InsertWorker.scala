package ecol.util

import akka.actor.Actor
import ecol.cassandra.model.TemperatureLog

class InsertWorker extends Actor {

  def receive = {
    case Message.InsertTemperature(ts, sensorAddress, temperatureValue) => {
      //println("[RCV message] - insert temperature: "+temperatureValue+", "+ sensorAddress)
      TemperatureLog.insertTemperature(ts, sensorAddress, temperatureValue)
    }
  }
}
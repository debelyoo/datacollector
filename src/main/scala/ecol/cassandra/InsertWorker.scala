package ecol.cassandra

import akka.actor.Actor

class InsertWorker extends Actor {

  def receive = {
    case Message.InsertTemperature(ts, sensorAddress, temperatureValue) => {
      //println("[RCV message] - insert temperature: "+temperatureValue+", "+ sensorAddress)
      AstyanaxConnector.insertTemperature(ts, sensorAddress, temperatureValue)
    }
  }
}
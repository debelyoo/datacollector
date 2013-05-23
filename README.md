datacollector
=============

A Scala backend to handle data collected from various sensors (temperature, GPS, gyro, and more).

The backend is basically a set of REST web services. It supports GET, POST, and PUT methods. 
The server is developed with the Spray framework (http://spray.io/)

The data to log is a collection of time series and I'm using Cassandra to store them. To interact with Cassandra, I'm using the Astyanax Java client (https://github.com/Netflix/astyanax). I intended to use Twitter's Cassie client (https://github.com/twitter/cassie) but it does not support Scala 2.10 (it needs scala 2.8).


# Prototype testing different data streaming libraries (Kafka, RabbitMQ, Vertx.eventBus())
#Development Environment

* Java 8
* Intellij 15
* maven 3.3.9
* Google Cloud Hosting ( free trial for kafka and rabbitmq servers)
* Free jenkins hosted on Google Clouds


## You can view the simulations in the test-integration module.

* The quick module contains the same simulation with smaller data file.
* The slow module uses your data file as data source.
* Open the project with Intellij and run it ur self. 
* The slow test will take long time to run ( > 1 hour) because the server is in the cloud. Except the VertxMessageTest will take around 5 minutes.
* The data process logic resides in data-processor module.

## You can view the result under test-integraton/src/test/resources/slow

* duplicateEvents.txt shows the number of duplicate events and their ID
* maxDevice.txt shows the device with max life time and the details list of lifetime of individual device.
* result.txt shows all uniquely different format for each type of events that it processed


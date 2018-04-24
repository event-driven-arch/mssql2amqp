# Subscribtion - publication by amqp from mssql to anywere
* golang realization of [ServiceBrokerListener](https://github.com/dyatchenko/ServiceBrokerListener)
* crossplatform and other pros of golang
## Instructions
* Run 
```bash
go run main.go -AMQP_URI="amqp://guest:guest@localhost:5672/"
```
* Create and subscribe to queue "REPLYTO-QUEUE"
* Publish message to queue "mssql-subscribe"
```go
{
  reply_to: "REPLYTO-QUEUE"
  body: JSON
}
```
JSON =
```json
{
  "ConnectionString": "Server=myServerAddress\myInstanceName;Database=<Database>;User Id=myUsername;Password=myPassword;",
  "DatabaseName"    : "<Database>",
  "TableName"       : "<Table>"
}
```
* Change data in Database Table
* Wait for messages in queue "REPLYTO-QUEUE"
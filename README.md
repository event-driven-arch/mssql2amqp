# Subscribtion - publication from mssql by amqp to anywere
* [stolen from ServiceBrokerListener](https://github.com/dyatchenko/ServiceBrokerListener)
* crossplatform by golang
## Instructions
* Run 
```bash
go run main.go -AMQP_URI="amqp://guest:guest@localhost:5672/"
```
* Subscribe to queue "REPLYTO-QUEUE"
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
  ConnectionString: ""
  DatabaseName    : ""
  TableName       : ""
}
```
* Change data in Database Table
* Wait for messages from queue "REPLYTO-QUEUE"
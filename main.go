package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"text/template"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/streadway/amqp"
)

var (
	amqpURI = flag.String("AMQP_URI", "amqp://guest:guest@localhost:5672/", "amqp://login:password@host:port/vhost")

	rabbitConn       *amqp.Connection
	rabbitCloseError chan *amqp.Error
	msgs             <-chan amqp.Delivery
	ch               *amqp.Channel
)

func main() {
	flag.Parse()
	defer ch.Close()
	defer rabbitConn.Close()
	rabbitCloseError = make(chan *amqp.Error)
	forever := make(chan bool)

	// run the callback in a separate thread
	go RabbitConnector(rabbitCloseError, rabbitConn, ch, setup)

	// establish the rabbitmq connection by sending
	// an error and thus calling the error callback
	rabbitCloseError <- amqp.ErrClosed
	log.Printf(" [*] Awaiting requests")
	<-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

// ConnectToRabbitMQ - Try to connect to the RabbitMQ server as
// long as it takes to establish a connection
func ConnectToRabbitMQ() *amqp.Connection {
	for {
		conn, err := amqp.Dial(*amqpURI)

		if err == nil {
			return conn
		}

		log.Println(err)
		log.Printf("Trying to reconnect to RabbitMQ at %s\n", *amqpURI)
		time.Sleep(2 * time.Second)
	}
}

// RabbitConnector re-establish the connection to RabbitMQ in case
// the connection has died
func RabbitConnector(rabbitCloseError chan *amqp.Error, rabbitConn *amqp.Connection, ch *amqp.Channel, fn func(conn *amqp.Connection, ch *amqp.Channel)) {
	var rabbitErr *amqp.Error

	for {
		rabbitErr = <-rabbitCloseError
		if rabbitErr != nil {
			log.Printf("\nConnecting to %s\n", *amqpURI)

			rabbitConn = ConnectToRabbitMQ()
			rabbitCloseError = make(chan *amqp.Error)
			rabbitConn.NotifyClose(rabbitCloseError)

			var err error
			ch, err = rabbitConn.Channel()
			failOnError(err, "Failed to open a channel")

			// run your setup process here
			// setup(rabbitConn, ch)
			fn(rabbitConn, ch)
		}
	}
}

func SubscribeQueue(ch *amqp.Channel, queue string) (<-chan amqp.Delivery, error) {
	q, err := ch.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when usused
		false, // exclusive
		false, // noWait
		nil,   // orderIDuments
	)
	failOnError(err, "Failed to QueueDeclare")
	fmt.Println("queue: ", q.Name)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // orderIDs
	)
	failOnError(err, "Failed to register a consumer")
	return msgs, err
}

func setup(conn *amqp.Connection, ch1 *amqp.Channel) {
	ch = ch1
	err := ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err = SubscribeQueue(ch, "mssql-subscribe")

	go func() {
		for d := range msgs {
			var req struct {
				ConnectionString string
				DatabaseName     string
				TableName        string
			}
			err := json.Unmarshal(d.Body, &req)
			failOnError(err, "json.Unmarshal")
			log.Printf(string(d.Body))
			l := Listener{ConnectionString: req.ConnectionString,
				DatabaseName: req.DatabaseName,
				TableName:    req.TableName,
				ReplyTo:      d.ReplyTo,
				Identity:     req.TableName,
			}
			l.start()
			d.Ack(false)
		}
	}()
}

// Listener ...
type Listener struct {
	ReplyTo          string
	ConnectionString string
	DatabaseName     string
	TableName        string
	SchemaName       string
	TriggerType                              string
	DetailsIncluded                          bool
	Detailed                                 string
	Identity                                 string
	ConversationQueueName                    string
	ConversationServiceName                  string
	ConversationTriggerName                  string
	InstallListenerProcedureName             string
	UninstallListenerProcedureName           string
	InstallServiceBrokerNotificationScript   string
	InstallNotificationTriggerScript         string
	UninstallNotificationTriggerScript       string
	UninstallServiceBrokerNotificationScript string
}

// #region Scripts

// #region Procedures

const SQL_PERMISSIONS_INFO = `
			DECLARE @msg VARCHAR(MAX)
			DECLARE @crlf CHAR(1)
			SET @crlf = CHAR(10)
			SET @msg = 'Current user must have following permissions: '
			SET @msg = @msg + '[CREATE PROCEDURE, CREATE SERVICE, CREATE QUEUE, SUBSCRIBE QUERY NOTIFICATIONS, CONTROL, REFERENCES] '
			SET @msg = @msg + 'that are required to start query notifications. '
			SET @msg = @msg + 'Grant described permissions with following script: ' + @crlf
			SET @msg = @msg + 'GRANT CREATE PROCEDURE TO [<username>];' + @crlf
			SET @msg = @msg + 'GRANT CREATE SERVICE TO [<username>];' + @crlf
			SET @msg = @msg + 'GRANT CREATE QUEUE  TO [<username>];' + @crlf
			SET @msg = @msg + 'GRANT REFERENCES ON CONTRACT::[DEFAULT] TO [<username>];' + @crlf
			SET @msg = @msg + 'GRANT SUBSCRIBE QUERY NOTIFICATIONS TO [<username>];' + @crlf
			SET @msg = @msg + 'GRANT CONTROL ON SCHEMA::[<schemaname>] TO [<username>];'

			PRINT @msg
`

/// T-SQL script-template which creates notification setup procedure.
/// {0} - database name.
/// {1} - setup procedure name.
/// {2} - service broker configuration statement.
/// {3} - notification trigger configuration statement.
/// {4} - notification trigger check statement.
/// {5} - table name.
/// {6} - schema name.
// installationProcedureScript := fmt.Sprintf(
// 	this.DatabaseName,
// 	this.InstallListenerProcedureName,
// 	strings.Replace(installServiceBrokerNotificationScript, "'", "''", -1),
// 	strings.Replace(installNotificationTriggerScript, "'", "''''", -1),
// 	strings.Replace(uninstallNotificationTriggerScript, "'", "''", -1),
// 	this.TableName,
// 	this.SchemaName)
const SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE = `
		USE [{{.DatabaseName}}]
		` + SQL_PERMISSIONS_INFO + `
		IF OBJECT_ID ('{{.SchemaName}}.{{.InstallListenerProcedureName}}', 'P') IS NULL
		BEGIN
			EXEC ('
				CREATE PROCEDURE {{.SchemaName}}.{{.InstallListenerProcedureName}}
				AS
				BEGIN
					-- Service Broker configuration statement
					{{.InstallServiceBrokerNotificationScript}}

					-- Notification Trigger check statement
					{{.UninstallNotificationTriggerScript}}

					-- Notification Trigger configuration statement
					DECLARE @triggerStatement NVARCHAR(MAX)
					DECLARE @select NVARCHAR(MAX)
					DECLARE @sqlInserted NVARCHAR(MAX)
					DECLARE @sqlDeleted NVARCHAR(MAX)

					SET @triggerStatement = N''{{.InstallNotificationTriggerScript}}''

					SET @select = STUFF((SELECT '','' + ''['' + COLUMN_NAME + '']''
										 FROM INFORMATION_SCHEMA.COLUMNS
										 WHERE DATA_TYPE NOT IN  (''text'',''ntext'',''image'',''geometry'',''geography'') 
										 AND TABLE_SCHEMA = ''{{.SchemaName}}'' AND TABLE_NAME = ''{{.TableName}}'' AND TABLE_CATALOG = ''{{.DatabaseName}}''
										 FOR XML PATH ('''')
										 ), 1, 1, '''')
					SET @sqlInserted =
						N''SET @retvalOUT = (SELECT '' + @select + N''
											 FROM INSERTED
											 FOR XML PATH(''''row''''), ROOT (''''inserted''''))''
					SET @sqlDeleted =
						N''SET @retvalOUT = (SELECT '' + @select + N''
											 FROM DELETED
											 FOR XML PATH(''''row''''), ROOT (''''deleted''''))''
					SET @triggerStatement = REPLACE(@triggerStatement
											 , ''%inserted_select_statement%'', @sqlInserted)
					SET @triggerStatement = REPLACE(@triggerStatement
											 , ''%deleted_select_statement%'', @sqlDeleted)

					EXEC sp_executesql @triggerStatement
				END
				')
		END
`

// #endregion

// #region ServiceBroker notification
/// T-SQL script-template which prepares database for ServiceBroker notification.
/// {0} - database name;
/// {1} - conversation queue name.
/// {2} - conversation service name.
/// {3} - schema name.
//-- Setup Service Broker
//IF EXISTS (SELECT * FROM sys.databases
//                    WHERE name = '{0}' AND (is_broker_enabled = 0 OR is_trustworthy_on = 0))
//BEGIN

//    ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
//    ALTER DATABASE [{0}] SET ENABLE_BROKER;
//    ALTER DATABASE [{0}] SET MULTI_USER WITH ROLLBACK IMMEDIATE

//    -- FOR SQL Express
//    ALTER AUTHORIZATION ON DATABASE::[{0}] TO [sa]
//    ALTER DATABASE [{0}] SET TRUSTWORTHY ON;

//END

// #endregion

// #region Notification Trigger

/// T-SQL script-template which creates notification trigger.
/// {0} - monitorable table name.
/// {1} - notification trigger name.
/// {2} - event data (INSERT, DELETE, UPDATE...).
/// {3} - conversation service name.
/// {4} - detailed changes tracking mode.
/// {5} - schema name.
/// %inserted_select_statement% - sql code which sets trigger "inserted" value to @retvalOUT variable.
/// %deleted_select_statement% - sql code which sets trigger "deleted" value to @retvalOUT variable.

// this.TableName,
// this.ConversationTriggerName,
// this.getTriggerTypeByListenerType(),
// this.ConversationServiceName,
// detailed,
// this.SchemaName)
const SQL_FORMAT_CREATE_NOTIFICATION_TRIGGER = `
		CREATE TRIGGER [{{.ConversationTriggerName}}]
		ON {{.SchemaName}}.[{{.TableName}}]
		AFTER {{.TriggerType}}
		AS

		SET NOCOUNT ON;

		--Trigger {{.TableName}} is rising...
		IF EXISTS (SELECT * FROM sys.services WHERE name = '{{.ConversationServiceName}}')
		BEGIN
			DECLARE @message NVARCHAR(MAX)
			SET @message = N'<root/>'

			IF ({{.Detailed}} EXISTS(SELECT 1))
			BEGIN
				DECLARE @retvalOUT NVARCHAR(MAX)

				%inserted_select_statement%

				IF (@retvalOUT IS NOT NULL)
				BEGIN SET @message = N'<root>' + @retvalOUT END

				%deleted_select_statement%

				IF (@retvalOUT IS NOT NULL)
				BEGIN
					IF (@message = N'<root/>') BEGIN SET @message = N'<root>' + @retvalOUT END
					ELSE BEGIN SET @message = @message + @retvalOUT END
				END

				IF (@message != N'<root/>') BEGIN SET @message = @message + N'</root>' END
			END

			--Beginning of dialog...
			DECLARE @ConvHandle UNIQUEIDENTIFIER
			--Determine the Initiator Service, Target Service and the Contract
			BEGIN DIALOG @ConvHandle
				FROM SERVICE [{{.ConversationServiceName}}] TO SERVICE '{{.ConversationServiceName}}' ON CONTRACT [DEFAULT] WITH ENCRYPTION=OFF, LIFETIME = 60;
			--Send the Message
			SEND ON CONVERSATION @ConvHandle MESSAGE TYPE [DEFAULT] (@message);
			--End conversation
			END CONVERSATION @ConvHandle;
		END
`

// #endregion

/// T-SQL script-template which returns all dependency identities in the database.
/// {0} - database name.
const SQL_FORMAT_GET_DEPENDENCY_IDENTITIES = `
		USE [{0}]

		SELECT REPLACE(name , 'ListenerService_' , '')
		FROM sys.services
		WHERE [name] like 'ListenerService_%';
`

// #endregion

// #region Forced cleaning of database

/// <summary>
/// T-SQL script-template which cleans database from notifications.
/// {0} - database name.
/// </summary>
const SQL_FORMAT_FORCED_DATABASE_CLEANING = `
		USE [{0}]

		DECLARE @db_name VARCHAR(MAX)
		SET @db_name = '{0}' -- provide your own db name

		DECLARE @proc_name VARCHAR(MAX)
		DECLARE procedures CURSOR
		FOR
		SELECT   sys.schemas.name + '.' + sys.objects.name
		FROM    sys.objects
		INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
		WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'sp_UninstallListenerNotification_%'

		OPEN procedures;
		FETCH NEXT FROM procedures INTO @proc_name

		WHILE (@@FETCH_STATUS = 0)
		BEGIN
		EXEC ('USE [' + @db_name + '] EXEC ' + @proc_name + ' IF (OBJECT_ID ('''
						+ @proc_name + ''', ''P'') IS NOT NULL) DROP PROCEDURE '
						+ @proc_name)

		FETCH NEXT FROM procedures INTO @proc_name
		END

		CLOSE procedures;
		DEALLOCATE procedures;

		DECLARE procedures CURSOR
		FOR
		SELECT   sys.schemas.name + '.' + sys.objects.name
		FROM    sys.objects
		INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
		WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'sp_InstallListenerNotification_%'

		OPEN procedures;
		FETCH NEXT FROM procedures INTO @proc_name

		WHILE (@@FETCH_STATUS = 0)
		BEGIN
		EXEC ('USE [' + @db_name + '] DROP PROCEDURE '
						+ @proc_name)

		FETCH NEXT FROM procedures INTO @proc_name
		END

		CLOSE procedures;
		DEALLOCATE procedures;
`

// #endregion
// #endregion

func (this Listener) start() {
	this.SchemaName = "dbo"
	this.ConversationQueueName = fmt.Sprintf("ListenerQueue_%s", this.Identity)
	this.ConversationServiceName = fmt.Sprintf("ListenerService_%s", this.Identity)
	this.ConversationTriggerName = fmt.Sprintf("tr_Listener_%s", this.Identity)
	this.InstallListenerProcedureName = fmt.Sprintf("sp_InstallListenerNotification_%s", this.Identity)
	this.UninstallListenerProcedureName = fmt.Sprintf("sp_UninstallListenerNotification_%s", this.Identity)
	log.Println(this)

	execInstallationProcedureScript := getScript("execInstallationProcedureScript", `
		USE [{{.DatabaseName}}]
		IF OBJECT_ID ('{{.SchemaName}}.{{.InstallListenerProcedureName}}', 'P') IS NOT NULL
			EXEC {{.SchemaName}}.{{.InstallListenerProcedureName}}
        `, this)
	ExecuteNonQuery(this.GetInstallNotificationProcedureScript(), this.ConnectionString)
	ExecuteNonQuery(this.GetUninstallNotificationProcedureScript(), this.ConnectionString)
	ExecuteNonQuery(execInstallationProcedureScript, this.ConnectionString)
	go func() {
		// COMMAND_TIMEOUT := 60000
		var commandText = getScript("", `
            DECLARE @ConvHandle UNIQUEIDENTIFIER
            DECLARE @message VARBINARY(MAX)
            USE [{{.DatabaseName}}]
            WAITFOR (RECEIVE TOP(1) @ConvHandle=Conversation_Handle
                        , @message=message_body FROM {{.SchemaName}}.[{{.ConversationQueueName}}]), TIMEOUT 30000;
            BEGIN TRY END CONVERSATION @ConvHandle; END TRY BEGIN CATCH END CATCH
    
            SELECT CAST(@message AS NVARCHAR(MAX))
    `, this)
		db, err := sql.Open("sqlserver", this.ConnectionString)
		failOnError(err, "sql.Open")
		defer db.Close()
		for {
			rows, err := db.Query(commandText)
			if err != nil {
				// log.Println(err.Error())
				// if err.Error() == "i/o timeout" {
				continue
				// }
				// log.Fatal(err)
			}
			defer rows.Close()
			for rows.Next() {
				var s sql.NullString
				if err := rows.Scan(&s); err != nil {
					// log.Fatal(err)
					log.Println(err)
					// return
				}
				if s.Valid && s.String != "" {
					log.Println(s.String)
					err = ch.Publish(
						"",           // exchange
						this.ReplyTo, // routing key
						false,        // mandatory
						false,        // immediate
						amqp.Publishing{
							ContentType: "text/plain",
							// CorrelationId: d.CorrelationId,
							Body: []byte(s.String),
						})
					if err != nil {
						log.Println("Failed to publish a message")
						return
					}
				}
			}
			if err := rows.Err(); err != nil {
				log.Println(err)
				return
			}
		}
	}()
}

func (l Listener) getTriggerTypeByListenerType() string {
	return "INSERT, UPDATE, DELETE"
}

func getScript(name string, text string, data interface{}) string {
	tmplt, err := template.New(name).Parse(text)
	failOnError(err, "template.New(name).Parse")
	buf := &bytes.Buffer{}
	err = tmplt.Execute(buf, data)
	failOnError(err, "tmplt.Execute")
	// log.Println(buf.String())
	return buf.String()
}

func (this Listener) GetInstallNotificationProcedureScript() string {
	s := getScript("installServiceBrokerNotificationScript", `
        -- Create a queue which will hold the tracked information
        IF NOT EXISTS (SELECT * FROM sys.service_queues WHERE name = '{{.ConversationQueueName}}')
            CREATE QUEUE {{.SchemaName}}.[{{.ConversationQueueName}}]
        -- Create a service on which tracked information will be sent
        IF NOT EXISTS(SELECT * FROM sys.services WHERE name = '{{.ConversationServiceName}}')
            CREATE SERVICE [{{.ConversationServiceName}}] ON QUEUE {{.SchemaName}}.[{{.ConversationQueueName}}] ([DEFAULT])
    `, this)
	this.InstallServiceBrokerNotificationScript = strings.Replace(s, "'", "''", -1)

	this.Detailed = "NOT"
	if this.DetailsIncluded {
		this.Detailed = ""
	}
	this.TriggerType = "INSERT, UPDATE, DELETE"
	s = getScript("installNotificationTriggerScript", SQL_FORMAT_CREATE_NOTIFICATION_TRIGGER, this)
	this.InstallNotificationTriggerScript = strings.Replace(s, "'", "''''", -1)

	s = getScript("uninstallNotificationTriggerScript", `
        IF OBJECT_ID ('{{.SchemaName}}.{{.ConversationTriggerName}}', 'TR') IS NOT NULL
        RETURN;
`, this)
	this.UninstallNotificationTriggerScript = strings.Replace(s, "'", "''", -1)

	return getScript("installationProcedureScript", SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE, this)
}

func (this Listener) GetUninstallNotificationProcedureScript() string {
	this.UninstallServiceBrokerNotificationScript = getScript("uninstallServiceBrokerNotificationScript", `
DECLARE @serviceId INT
SELECT @serviceId = service_id FROM sys.services
WHERE sys.services.name = '{{.ConversationServiceName}}'

DECLARE @ConvHandle uniqueidentifier
DECLARE Conv CURSOR FOR
SELECT CEP.conversation_handle FROM sys.conversation_endpoints CEP
WHERE CEP.service_id = @serviceId AND ([state] != 'CD' OR [lifetime] > GETDATE() + 1)

OPEN Conv;
FETCH NEXT FROM Conv INTO @ConvHandle;
WHILE (@@FETCH_STATUS = 0) BEGIN
    END CONVERSATION @ConvHandle WITH CLEANUP;
    FETCH NEXT FROM Conv INTO @ConvHandle;
END
CLOSE Conv;
DEALLOCATE Conv;

-- Droping service and queue.
IF  EXISTS (SELECT * FROM sys.services WHERE name = N'{{.ConversationServiceName}}')
    DROP SERVICE [{{.ConversationServiceName}}];
IF OBJECT_ID ('{{.SchemaName}}.{{.ConversationQueueName}}', 'SQ') IS NOT NULL
    DROP QUEUE {{.SchemaName}}.[{{.ConversationQueueName}}];
`, this)
	this.UninstallNotificationTriggerScript = getScript("uninstallNotificationTriggerScript", `
                IF OBJECT_ID ('{{.SchemaName}}.{{.ConversationTriggerName}}', 'TR') IS NOT NULL
                    DROP TRIGGER {{.SchemaName}}.[{{.ConversationTriggerName}}];
                `, this)
	this.UninstallServiceBrokerNotificationScript = strings.Replace(this.UninstallServiceBrokerNotificationScript, "'", "''", -1)
	this.UninstallNotificationTriggerScript = strings.Replace(this.UninstallNotificationTriggerScript, "'", "''", -1)
	return getScript("uninstallationProcedureScript", `
		USE [{{.DatabaseName}}]
		`+SQL_PERMISSIONS_INFO+`
		IF OBJECT_ID ('{{.SchemaName}}.{{.UninstallListenerProcedureName}}', 'P') IS NULL
		BEGIN
			EXEC ('
				CREATE PROCEDURE {{.SchemaName}}.{{.UninstallListenerProcedureName}}
				AS
				BEGIN
					-- Notification Trigger drop statement.
					{{.UninstallNotificationTriggerScript}}

					-- Service Broker uninstall statement.
					{{.UninstallServiceBrokerNotificationScript}}

					IF OBJECT_ID (''{{.SchemaName}}.{{.InstallListenerProcedureName}}'', ''P'') IS NOT NULL
						DROP PROCEDURE {{.SchemaName}}.{{.InstallListenerProcedureName}}

					DROP PROCEDURE {{.SchemaName}}.{{.UninstallListenerProcedureName}}
				END
				')
		END
`, this)
}

// ExecuteNonQuery ...
func ExecuteNonQuery(commandText string, connectionString string) {
	db, err := sql.Open("sqlserver", connectionString)
	failOnError(err, "sql.Open")
	defer db.Close()

	_, err = db.Exec(commandText)
	failOnError(err, "sql.Open")
}

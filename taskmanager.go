package main

import (
	"github.com/streadway/amqp"
	"log"
	"fmt"
	"encoding/json"
	"gopkg.in/redis.v3"
	"os"
	 
)
const RABBITMQ_IP_VAR = "AMQ_PORT_5672_TCP_ADDR"
const RABBITMQ_PORT_VAR = "AMQ_PORT_5672_TCP_PORT"
const REDIS_IP_VAR = "DB_PORT_6379_TCP_ADDR"
const REDIS_PORT_VAR = "DB_PORT_6379_TCP_PORT"


const RABBITMQ_USER = "guest"
const RABBITMQ_PWD = "guest"

const RABBITMQ_PROCESS_QUEUE = "PROCESS"
const RABBITMQ_TASK_EXCHANGE ="EXCHANGE_TASK"
const RABBITMQ_TASK_EXCHANGE_END ="EXCHANGE_TASK_END"
const RABBITMQ_ENDTASK_TOPIC = "END_TASK"

const STEPID_ERROR="9999999"



/********* TASK MESSAGE **********/

type TaskMessage struct {
	ContextID string `json:"contextId"`
	StepID    string `json:"stepId"`
	redisClient *redis.Client
}

 func (tm *TaskMessage) getContextVariable(varName string)  (string,error) {
	 return tm.redisClient.Get(tm.ContextID + "_" + varName).Result()
}

func (tm *TaskMessage) setContextResult(varName string,value string)  error {
	return tm.redisClient.Set(tm.ContextID + "_" + varName,value,0).Err()
}





/********* TASK MANAGER **********/

type TaskManager struct {
     conn *amqp.Connection
	 channel  *amqp.Channel
	 channelEnd  *amqp.Channel
	 processName string
	 queueName string
	 redisClient *redis.Client
	
}


func  QueueConsumerFactory(processName string) (*TaskManager,error) {
	
	log.Printf("Initiating task %s\n", processName)
	RABBITMQ_ADDR:=os.Getenv(RABBITMQ_IP_VAR)
	if(RABBITMQ_ADDR=="") {
		RABBITMQ_ADDR="192.168.1.121"
	}	
 	RABBITMQ_PORT:=os.Getenv(RABBITMQ_PORT_VAR)
	if(RABBITMQ_PORT=="") {
		RABBITMQ_PORT="5672"
	}
	url := "amqp://" + RABBITMQ_PWD +":" + RABBITMQ_PWD + "@" + RABBITMQ_ADDR + ":" + RABBITMQ_PORT
	conn, err := amqp.Dial(url)
    if err != nil {
		defer conn.Close()
        return nil,err
    }
	log.Printf("Connected to queue: %s\n", url)
    
	//build channels in the connection
    channelIN, err := conn.Channel()
        if err != nil {
		defer channelIN.Close()
        return nil,err
    }
	channelOUT, err := conn.Channel()
        if err != nil {
		defer channelOUT.Close()
        return nil,err
    }
	log.Printf("Channels created\n")
	//create exchange in channels
	 err = channelIN.ExchangeDeclare(
                RABBITMQ_TASK_EXCHANGE, // name
                "direct",  // type
                true,         // durable
                false,        // auto-deleted
                false,        // internal
                false,        // no-wait
                nil,          // arguments
      )
      if err != nil {
		defer channelIN.Close()
        return nil,err
	  }	
	  //create exchange in channels
	 err = channelOUT.ExchangeDeclare(
                RABBITMQ_TASK_EXCHANGE_END, // name
                "direct",  // type
                true,         // durable
                false,        // auto-deleted
                false,        // internal
                false,        // no-wait
                nil,          // arguments
      )
      if err != nil {
		defer channelIN.Close()
        return nil,err
	  }
	log.Printf("Exchanges created\n")
	
    //declare queuel
  	q, err := channelIN.QueueDeclare(
               "CUA_PROVA_TASCA",    // name
               false, // durable
               false, // delete when usused
               false,  // exclusive
               false, // no-wait
               nil,   // arguments
       )
	if err != nil {
		defer channelIN.Close()
       	return nil,err
  	}
	log.Printf("Queue declared\n")
	
	  //bind queue
	  err = channelIN.QueueBind(
                        q.Name,   // queue name
                        processName,            // routing key
                        RABBITMQ_TASK_EXCHANGE, // exchange
                        false,
                        nil)
	if err != nil {
		defer channelIN.Close()
       	return nil,err
  	}
	log.Printf("Queue %s binded to key %s\n",q.Name,processName)
	
	//REDIS CLIENT
	REDIS_IP:=os.Getenv(REDIS_IP_VAR)
	if(REDIS_IP=="") {
		REDIS_IP="192.168.1.121"
	}	
 	REDIS_PORT:=os.Getenv(REDIS_PORT_VAR)
	if(REDIS_PORT=="") {
		REDIS_PORT="6379"
	}
	log.Printf("redis url : %s:%s\n",REDIS_IP,REDIS_PORT)
	clientRedis := redis.NewClient(&redis.Options{
	    Addr:     REDIS_IP + ":" + REDIS_PORT,
	    Password: "", // no password set
	    DB:       0,  // use default DB
	})
	
	_, err = clientRedis.Ping().Result()
	if err != nil {
        	return nil,err
	  }
	log.Printf("Connected to redis: %s\n",REDIS_IP + ":" + REDIS_PORT)					
	// channel end		
	fmt.Printf("%s is waiting, 'your wish is my command.....\n'",processName)			
	 return &TaskManager{conn,channelIN,channelOUT,processName,q.Name,clientRedis},nil
}


func   (q *TaskManager) getTaskMessage(msg amqp.Delivery) (TaskMessage,error) {
    var m TaskMessage
	err := json.Unmarshal([]byte(msg.Body), &m)	
	if(err!=nil) {
		return m,err
	}
	m.redisClient=q.redisClient
	return m,nil
}

func   (q *TaskManager) buildErrorTaskMessage() (TaskMessage) {
	return TaskMessage{StepID:STEPID_ERROR,ContextID:"",redisClient:nil}
}

func (q *TaskManager) sendEndMissage(tm TaskMessage) error {
	
	data:=fmt.Sprintf(`{"stepId":"%s","error":""}`,tm.StepID)
	return q.channelEnd.Publish(
                RABBITMQ_TASK_EXCHANGE_END, // exchange
                RABBITMQ_ENDTASK_TOPIC,     // routing key
                false,  // mandatory
                false,  // immediate
                amqp.Publishing{
                        ContentType: "text/plain",
                        Body:        []byte(data),
                })
}

func (q *TaskManager) sendErrorMissage(tm TaskMessage,err error) error {
	
	data:=fmt.Sprintf(`{"stepId":"%s","error":%s,}`,tm.StepID,err)
	return q.channelEnd.Publish(
                RABBITMQ_TASK_EXCHANGE_END, // exchange
                RABBITMQ_ENDTASK_TOPIC,     // routing key
                false,  // mandatory
                false,  // immediate
                amqp.Publishing{
                        ContentType: "text/plain",
                        Body:        []byte(data),
                })
}

func (q *TaskManager) consume() (<-chan amqp.Delivery,error) {
	return q.channel.Consume(
		q.queueName, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
}




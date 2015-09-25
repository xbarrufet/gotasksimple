// gotasksimple project main.go
package main

import (
	"fmt"
	"os"
	"errors"

)

const TASK_NAME_VAR="TASK_NAME"

func main() {
	TASK_NAME:=os.Getenv(TASK_NAME_VAR)
	if(TASK_NAME=="") {
		TASK_NAME="GOTASKSIMPLE"
	}
		
	taskManager,err := QueueConsumerFactory(TASK_NAME)
	if(err!=nil) {
		panic(fmt.Sprintf("Error creating task manager %s",err))
	}
	
	msg,err:=taskManager.consume()
	if(err!=nil) {		
		panic(fmt.Sprintf("Error consuming messages %s",err))
	}
	go func() {
		for d := range msg {
			fmt.Printf("Msg received ...\n")
			d.Ack(false)
			taskMessage,err:=taskManager.getTaskMessage(d)
			if err!=nil {
	  			taskMessage=taskManager.buildErrorTaskMessage()
				err=taskManager.sendErrorMissage(taskMessage,err)
				if err!=nil {
	  				panic(fmt.Sprintf("Error sending error end msg %s",err))
				}
			} 		  
			// do stuff
			 err=concat(taskMessage)	
			 if(err!=nil) {
				err=errors.New(fmt.Sprintf("Error on concat process: %s",err))
				err=taskManager.sendErrorMissage(taskMessage,err)				
			}
			fmt.Printf("Mjob done, sending END missage ...\n")
			err=taskManager.sendEndMissage(taskMessage)
			if err!=nil {
	  			panic(fmt.Sprintf("Error sending end msg %s",err))
			}
			
		}
	}()
	
	forever := make(chan bool)	
	<-forever
	
}


func concat(taskMessage TaskMessage) error {
	cad1,err:=taskMessage.getContextVariable("cad1")
	if(err!=nil) {
		return errors.New(fmt.Sprintf("Error reading context variable cad1: %s",err))
	}				
	cad2,err:=taskMessage.getContextVariable("cad2")
	if(err!=nil) {
		return errors.New(fmt.Sprintf("Error reading context variable cad2: %s",err))
	}
	taskMessage.setContextResult("cadOutput",cad1 + cad2)
	return nil
}

package main

import (
	"context"
	"fmt"
	"time"
	"strings"
	"github.com/go-redis/redis/v8"
)

const KEYPRE string = "TINYTASK";
var redisClient *redis.Client
var ctx = context.Background()

func taskInit() bool {
	redisClient = redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})
          
	_, err := redisClient.Do(ctx, "CONFIG", "SET", "notify-keyspace-events", "Ex").Result()
	if err != nil {
		fmt.Printf("unable to set keyspace events %v\n", err.Error())
		return false
	}

	pubsub := redisClient.Subscribe(ctx, "__keyevent@0__:expired")

	go func(redis.PubSub) {
		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				fmt.Printf("unable to ReceiveMessage %v\n", err.Error())
				time.Sleep(1 * time.Second)
				continue
			}

			key := msg.Payload
			xr := strings.Split(key, ":")
			if xr[0] != KEYPRE { continue }
	
			_, errnx := redisClient.SetNX(ctx, key, "L", 100 * time.Second).Result()
			if errnx != nil { continue }  // Mutex, 避免被多个接收者多次执行
	
			key2 :=  KEYPRE + "2:" + xr[1]
			data, err := redisClient.Get(ctx, key2).Result()
			if err != nil { continue }
	
			// consume here
			fmt.Println("task", xr[1], data)
	
			redisClient.Del(ctx, key)
			redisClient.Del(ctx, key2)
		}
	}(*pubsub)

	return true
}

func taskAdd(delaySecond time.Duration, taskId string, taskData string) bool {
    key := KEYPRE + ":" + taskId
	err := redisClient.Set(ctx, key, "1", delaySecond).Err()
    if err != nil {
		fmt.Printf("unable to set taskAdd %v\n", err.Error())
		return false
    }

    key2 := KEYPRE + "2:" + taskId
	err2 := redisClient.Set(ctx, key2, taskData, delaySecond + 100 * time.Second).Err()
    if err2 != nil {
        fmt.Printf("unable to set taskAdd %v\n", err2.Error())
		return false
    }

	fmt.Println("New Task", taskId, "after", delaySecond)
	return true
}

func main() {
	taskInit()
    taskAdd(2 * time.Second, "dodo1", "222222")
	taskAdd(5 * time.Second, "dodo2", "555555")
	time.Sleep(7 * time.Second)
}
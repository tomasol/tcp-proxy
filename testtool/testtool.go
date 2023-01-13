package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync/atomic"
	"time"
)

// lower ascii string
func randomString(len int64) string {
	ran_str := make([]byte, len)
	for i := int64(0); i < len; i++ {
		ran_str[i] = byte('a' + rand.Intn(25))
	}
	return string(ran_str)
}

var connected, sentfromall, rcvdfromall, failed int64

func main() {
	host := flag.String("host", "localhost", "Server host")
	durationSecs := flag.Int("duration", 10, "Execution time in seconds")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent clients")
	payloadLen := flag.Int64("payload", 10, "Length of random ascii payload")
	flag.Parse()
	ports := flag.Args()
	if len(ports) == 0 {
		fmt.Println("No ports specified. Usage: testtool 5001 5200")
		flag.PrintDefaults()
		os.Exit(1)
	}

	flag.Parse()
	timeoutDuration := time.Duration(*durationSecs) * time.Second
	timeout := time.After(timeoutDuration)
	for i := 0; i < *concurrency; i++ {
		go func(i int, address string) {
			conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", address, ports[(i%len(ports))]))
			if err != nil {
				fmt.Printf("[%d] Connect failed: %v\n", i, err)
				atomic.AddInt64(&failed, 1)
				return
			}
			atomic.AddInt64(&connected, 1)
			for {
				sent := randomString(*payloadLen-1) + "\n"
				_, err := fmt.Fprint(conn, sent)
				if err != nil {
					fmt.Printf("[%d] Send failed: %v\n", i, err)
					atomic.AddInt64(&failed, 1)
					return
				}
				atomic.AddInt64(&sentfromall, 1)
				_, err = bufio.NewReader(conn).ReadString('\n')
				if err != nil {
					fmt.Printf("[%d] Read failed: %v\n", i, err)
					atomic.AddInt64(&failed, 1)
					return
				}
				atomic.AddInt64(&rcvdfromall, 1)
			}
		}(i, *host)
	}

	<-timeout
	sent := atomic.LoadInt64(&sentfromall)
	rcvd := atomic.LoadInt64(&rcvdfromall)
	fmt.Printf("Execution completed.\nConnected: %d\nSent: %d messages, %d bytes\nReceived: %d messages, %d bytes\nFailed: %d\n",
		atomic.LoadInt64(&connected),
		sent,
		*payloadLen*sent,
		rcvd,
		*payloadLen*rcvd,
		atomic.LoadInt64(&failed))
}

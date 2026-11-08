
package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	serverAddress = "127.0.0.1"
	basePort      = 9000
	dataSize      = 10 * 1024 * 1024
)

func createData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

func startReceiver(port int, wg *sync.WaitGroup) {
	defer wg.Done()

	listener, err := net.Listen("tcp", serverAddress+":"+strconv.Itoa(port))
	if err != nil {
		return
	}
	defer listener.Close()

	conn, err := listener.Accept()
	if err != nil {
		return
	}
	defer conn.Close()

	buffer := make([]byte, 32*1024)

	for {
		_, err := conn.Read(buffer)
		if err != nil {
			break
		}
	}
}

func directTransfer(port int, data []byte) error {
	conn, err := net.Dial("tcp", serverAddress+":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Write(data)
	return err
}

func measureDTT(nodes int) (float64, error) {
	var wg sync.WaitGroup

	for i := 0; i < nodes; i++ {
		wg.Add(1)
		go startReceiver(basePort+i, &wg)
	}

	time.Sleep(300 * time.Millisecond)

	data := createData(dataSize)
	start := time.Now()

	for i := 0; i < nodes; i++ {
		err := directTransfer(basePort+i, data)
		if err != nil {
			return 0, err
		}
	}

	elapsed := time.Since(start).Seconds() * 1000

	wg.Wait()
	return elapsed, nil
}

func main() {
	clusterSizes := []int{3, 5, 7, 9, 11}

	fmt.Println("Direct Transfer Time Measurements")
	fmt.Println("---------------------------------")

	for _, nodes := range clusterSizes {
		var total float64
		trials := 5

		for trial := 1; trial <= trials; trial++ {
			value, err := measureDTT(nodes)
			if err != nil {
				fmt.Println("Transfer error:", err)
				os.Exit(1)
			}
			total += value

			fmt.Printf("Nodes: %d | Trial: %d | DTT: %.3f ms\n",
				nodes, trial, value)
		}

		fmt.Printf("Nodes: %d | Average DTT: %.3f ms\n\n",
			nodes, total/float64(trials))
	}
}

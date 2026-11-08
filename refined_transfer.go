
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
	basePort      = 10000
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

func sendChunk(port int, chunk []byte, wg *sync.WaitGroup,
	errCh chan error) {
	defer wg.Done()

	conn, err := net.Dial("tcp", serverAddress+":"+strconv.Itoa(port))
	if err != nil {
		errCh <- err
		return
	}
	defer conn.Close()

	_, err = conn.Write(chunk)
	if err != nil {
		errCh <- err
	}
}

func refinedTransfer(nodes int, data []byte) error {
	var wg sync.WaitGroup
	errCh := make(chan error, nodes)

	chunkSize := (len(data) + nodes - 1) / nodes

	for i := 0; i < nodes; i++ {
		start := i * chunkSize
		end := start + chunkSize

		if end > len(data) {
			end = len(data)
		}

		if start >= len(data) {
			break
		}

		wg.Add(1)
		go startReceiver(basePort+i, &wg)
	}

	time.Sleep(300 * time.Millisecond)

	var senderWG sync.WaitGroup

	for i := 0; i < nodes; i++ {
		start := i * chunkSize
		end := start + chunkSize

		if end > len(data) {
			end = len(data)
		}

		if start >= len(data) {
			break
		}

		senderWG.Add(1)
		go sendChunk(basePort+i, data[start:end],
			&senderWG, errCh)
	}

	senderWG.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	wg.Wait()
	return nil
}

func measureRTT(nodes int) (float64, error) {
	data := createData(dataSize)

	start := time.Now()
	err := refinedTransfer(nodes, data)
	if err != nil {
		return 0, err
	}

	elapsed := time.Since(start).Seconds() * 1000
	return elapsed, nil
}

func main() {
	clusterSizes := []int{3, 5, 7, 9, 11}

	fmt.Println("Refined Transfer Time Measurements")
	fmt.Println("-----------------------------------")

	for _, nodes := range clusterSizes {
		var total float64
		trials := 5

		for trial := 1; trial <= trials; trial++ {
			value, err := measureRTT(nodes)
			if err != nil {
				fmt.Println("Transfer error:", err)
				os.Exit(1)
			}

			total += value

			fmt.Printf("Nodes: %d | Trial: %d | RTT: %.3f ms\n",
				nodes, trial, value)
		}

		fmt.Printf("Nodes: %d | Average RTT: %.3f ms\n\n",
			nodes, total/float64(trials))
	}
}

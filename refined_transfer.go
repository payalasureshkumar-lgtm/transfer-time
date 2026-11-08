
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type Node struct {
	ID      int
	Address string
	Port    string
}

var nodes = []Node{
	{1, "<NODE_1_IP>", "9001"},
	{2, "<NODE_2_IP>", "9002"},
	{3, "<NODE_3_IP>", "9003"},
	{4, "<NODE_4_IP>", "9004"},
	{5, "<NODE_5_IP>", "9005"},
	{6, "<NODE_6_IP>", "9006"},
	{7, "<NODE_7_IP>", "9007"},
	{8, "<NODE_8_IP>", "9008"},
	{9, "<NODE_9_IP>", "9009"},
	{10, "<NODE_10_IP>", "9010"},
	{11, "<NODE_11_IP>", "9011"},
}

const (
	dataSize = 10 * 1024 * 1024
	trials   = 5
)

func createData(size int) []byte {
	data := make([]byte, size)

	for i := range data {
		data[i] = byte(i % 256)
	}

	return data
}

func startReceiver(port string) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Receiver error:", err)
		return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	var size uint64

	err := binary.Read(conn, binary.BigEndian, &size)
	if err != nil || size == 0 || size > dataSize {
		return
	}

	received, err := io.CopyN(io.Discard, conn, int64(size))
	if err != nil || uint64(received) != size {
		return
	}

	err = binary.Write(conn, binary.BigEndian, uint8(1))
	if err != nil {
		return
	}

	fmt.Printf("Received: %d bytes\n", received)
}

func sendChunk(
	node Node,
	chunk []byte,
	wg *sync.WaitGroup,
	errCh chan error,
) {
	defer wg.Done()

	address := net.JoinHostPort(node.Address, node.Port)

	conn, err := net.DialTimeout(
		"tcp",
		address,
		10*time.Second,
	)
	if err != nil {
		errCh <- err
		return
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(60 * time.Second))

	err = binary.Write(
		conn,
		binary.BigEndian,
		uint64(len(chunk)),
	)
	if err != nil {
		errCh <- err
		return
	}

	written, err := io.Copy(conn, &sliceReader{data: chunk})
	if err != nil || written != int64(len(chunk)) {
		errCh <- fmt.Errorf("incomplete transfer to node %d", node.ID)
		return
	}

	var acknowledgement uint8

	err = binary.Read(conn, binary.BigEndian, &acknowledgement)
	if err != nil || acknowledgement != 1 {
		errCh <- fmt.Errorf("acknowledgement failed for node %d", node.ID)
	}
}

type sliceReader struct {
	data []byte
	pos  int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}

	n := copy(p, r.data[r.pos:])
	r.pos += n

	return n, nil
}

func refinedTransfer(
	clusterSize int,
	data []byte,
) (float64, error) {
	var wg sync.WaitGroup
	errCh := make(chan error, clusterSize)

	chunkSize := (len(data) + clusterSize - 1) / clusterSize

	startTime := time.Now()

	for i := 0; i < clusterSize; i++ {
		start := i * chunkSize
		end := start + chunkSize

		if end > len(data) {
			end = len(data)
		}

		if start >= len(data) {
			break
		}

		wg.Add(1)

		go sendChunk(
			nodes[i],
			data[start:end],
			&wg,
			errCh,
		)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return 0, err
		}
	}

	elapsed := time.Since(startTime).Seconds() * 1000

	return elapsed, nil
}

func runReceivers() {
	for _, node := range nodes {
		go startReceiver(node.Port)
	}
}

func runSender() {
	clusterSizes := []int{3, 5, 7, 9, 11}
	data := createData(dataSize)

	fmt.Println("REFINED TRANSFER TIME (RTT)")
	fmt.Println("===========================")

	for _, clusterSize := range clusterSizes {
		var total float64

		fmt.Printf("\nCluster Size: %d Nodes\n", clusterSize)

		for trial := 1; trial <= trials; trial++ {
			value, err := refinedTransfer(clusterSize, data)

			if err != nil {
				fmt.Println("Transfer error:", err)
				continue
			}

			total += value

			fmt.Printf(
				"Trial: %d | RTT: %.3f ms\n",
				trial,
				value,
			)
		}

		average := total / float64(trials)

		fmt.Printf(
			"Average RTT: %.3f ms\n",
			average,
		)
	}
}

func main() {
	if len(os.Args) > 1 {
		if os.Args[1] == "receiver" && len(os.Args) == 3 {
			port := os.Args[2]

			if _, err := strconv.Atoi(port); err != nil {
				fmt.Println("Invalid port")
				return
			}

			startReceiver(port)
			return
		}
	}

	runReceivers()
	runSender()
}

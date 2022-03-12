package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
)

var fname string // File name containing list of integers
var fsize int64  // Size of file
var M int64      // Number of worker threads

// Structure of message passed from coordinater to worker
// Contains information of fragment which worker need to process
type Fragment struct {
	Datafile string
	Start    int64
	End      int64
}

// Structure of response message from worker
type WorkerResponse struct {
	Psum   *big.Int
	Pcount int64
	Prefix int64
	Suffix int64
	Start  int64
	End    int64
}

// Main function
func main() {
	initializeArgs()
	writer := tabwriter.NewWriter(os.Stdout, 10, 8, 1,
		'\t', tabwriter.AlignRight)
	fmt.Println("\nWorker request fragments: {datafile, start, end}")
	fmt.Println("Worker Response: {psum, pcount, prefix, suffix, start, end}")
	fmt.Println()
	fmt.Fprintln(writer, "Average\t:", calcAverage())
	fmt.Fprintln(writer)
	writer.Flush()
}

// Function to intialize M, fname, and fsize
func initializeArgs() {
	if len(os.Args) != 3 {
		panic(`Invalid command line arguments, Provide exactly 2 arguments -
               M (Number of threads to spawn)
               fname (Input file path)
               Follow the sequence i.e. M followed by fname`)
	}

	fname = os.Args[2]
	file, err := os.Stat(fname)
	isSuccess(err)
	fsize = int64(file.Size())

	M, err = strconv.ParseInt(os.Args[1], 10, 64)
	isSuccess(err)
}

// Function to check if the last call was success
func isSuccess(err error) {
	if err != nil {
		panic(err)
	}
}

// Coordinater : Function to spawn worker threads
// and calculate average by aggregating result
func calcAverage() *big.Float {
	writer := tabwriter.NewWriter(os.Stdout, 10, 8, 1,
		'\t', tabwriter.AlignRight)
	// Channel for communication between workers and coordinater
	ch := make(chan []byte)

	// Spawn worker threads
	spawnWorkers(ch)

	total := big.NewInt(0)
	var count int64 = 0

	// aggregate results and calculate total
	for i := 0; i < int(M); i++ {
		var workerResp WorkerResponse
		err := json.Unmarshal(<-ch, &workerResp)
		isSuccess(err)

		fmt.Println(workerResp)
		total.Add(total, workerResp.Psum)
		total.Add(total, big.NewInt(workerResp.Prefix))
		total.Add(total, big.NewInt(workerResp.Suffix))
		count += workerResp.Pcount + 2
	}

	fmt.Fprintln(writer, "\nTotal\t:", total)
	fmt.Fprintln(writer, "Total Numbers\t:", count)
	writer.Flush()

	// calculate average
	avg := new(big.Float).Quo(new(big.Float).SetInt(total),
		new(big.Float).SetInt(big.NewInt((count))))
	return avg
}

// Function to spawn M worker threads and distibute data
func spawnWorkers(ch chan []byte) {
	partsize := fsize / M

	var start int64 = 0
	var end int64 = 0

	var i int64
	for i = 1; i < M+1; i++ {
		// Get data fragment to work upon
		start, end = getFragment(partsize, i, start)
		fragmentJson, err := json.Marshal(Fragment{fname, start, end})
		isSuccess(err)
		go worker(fragmentJson, ch) // spwan thread
		start = end
	}
}

// Function to calculate data fragment workload for each thread
func getFragment(partsize int64, fragmentNo int64, start int64) (int64, int64) {
	// Case: last data fragment
	if fragmentNo == M {
		return start, fsize
	}
	fhndl, err := os.Open(fname)
	isSuccess(err)

	lastByte := partsize * fragmentNo
	for {
		_, err := fhndl.Seek(lastByte, 0)
		isSuccess(err)

		lastByteData := make([]byte, 1)
		_, err = fhndl.Read(lastByteData)
		isSuccess(err)

		// Check if the last byte from fragment is space
		// first condition from OR is just corner case might not hit at all
		if lastByte == fsize || string(lastByteData) == " " {
			break
		}
		lastByte++
	}

	end := lastByte

	return start, end

}

// Worker threads implementation
func worker(fragmentInfo []byte, ch chan []byte) {
	var workerFragment Fragment
	err := json.Unmarshal(fragmentInfo, &workerFragment)
	isSuccess(err)

	fmt.Println(workerFragment)
	ch <- calcFragmentTotal(workerFragment.Datafile,
		workerFragment.Start,
		workerFragment.End)
}

// Function to calculate prefix, suffix, and partial sum of each fragment
func calcFragmentTotal(datafile string, start int64, end int64) []byte {
	fhndl, err := os.Open(datafile)
	isSuccess(err)
	defer fhndl.Close()

	_, err = fhndl.Seek(start, 0)
	isSuccess(err)

	data := make([]byte, end-start)
	_, err = fhndl.Read(data)
	isSuccess(err)

	numbers := strings.Fields(string(data))

	prefix, err := strconv.ParseInt(numbers[0], 10, 64)
	isSuccess(err)

	suffix, err := strconv.ParseInt(numbers[len(numbers)-1], 10, 64)
	isSuccess(err)

	// Loop over all numbers except prefix and suffix to add to partial sum
	psum := big.NewInt(0)
	var i int
	for i = 1; i < len(numbers)-1; i++ {
		number, err := strconv.ParseInt(numbers[i], 10, 64)
		isSuccess(err)
		psum.Add(psum, big.NewInt(int64(number)))
	}

	response := WorkerResponse{psum, int64(i - 1), prefix, suffix, start, end}
	responseJson, err := json.Marshal(response)
	isSuccess(err)

	return responseJson
}

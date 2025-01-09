package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
)

func main() {
	mainWg := sync.WaitGroup{}
	mainWg.Add(1)
	go Merger("result.txt", &mainWg)
	mainWg.Wait()
}

func GetDataFromFileInChannel(fileName string, sourceCh chan<- int, mainWg *sync.WaitGroup, dataFilesWg *sync.WaitGroup) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	defer mainWg.Done()
	defer dataFilesWg.Done()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if no, err := strconv.Atoi(line); err == nil {
			sourceCh <- no
		}
	}
}

func SplitAndSendToChan(sourceCh <-chan int, evenDataCh chan<- int, oddDataCh chan<- int) {
	defer close(evenDataCh)
	defer close(oddDataCh)
	for no := range sourceCh {
		if no%2 == 0 {
			evenDataCh <- no
		} else {
			oddDataCh <- no
		}
	}
}

func AggregateData(dataCh <-chan int, resultCh chan<- int) {
	total := 0
	for no := range dataCh {
		total += no
	}
	resultCh <- total
}

func Merger(fileName string, mainWg *sync.WaitGroup) {
	// Channel to read data from files
	sourceCh := make(chan int)

	// Channels for even and odd numbers
	evenCh := make(chan int)
	oddCh := make(chan int)

	// Channels for even and odd results
	evenResultCh := make(chan int)
	oddResultCh := make(chan int)

	// Close result channels
	defer close(evenResultCh)
	defer close(oddResultCh)

	// Once merger is done, main wait group is done
	defer mainWg.Done()

	// Create file to write the result
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	// Wait group to wait for all data files to be read
	dataFilesWg := sync.WaitGroup{}
	dataFiles := []string{"data1.dat", "data2.dat"}
	for _, dataFile := range dataFiles {
		mainWg.Add(1)
		dataFilesWg.Add(1)
		go GetDataFromFileInChannel(dataFile, sourceCh, mainWg, &dataFilesWg)
	}

	// Close source channel once all data files are read
	go func() {
		dataFilesWg.Wait()
		close(sourceCh)
	}()

	go SplitAndSendToChan(sourceCh, evenCh, oddCh)
	go AggregateData(evenCh, evenResultCh)
	go AggregateData(oddCh, oddResultCh)

	// Wait for even and odd results to be written to file
	for range 2 {
		select {
		case evenResult := <-evenResultCh:
			fmt.Fprintln(file, fmt.Sprintf("Even Total: %d", evenResult))
		case oddResult := <-oddResultCh:
			fmt.Fprintln(file, fmt.Sprintf("Odd Total: %d", oddResult))
		}
	}
}

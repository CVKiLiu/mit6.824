package myTest

import (
	"fmt"
	"strconv"
)

func makeArray() {
	intArray := make([]int, 0, 100)
	fmt.Println(len(intArray), " ", cap(intArray))
	intSlice := intArray[:]
	fmt.Println(len(intSlice), " ", cap(intSlice), "")
	fmt.Println(intSlice, " ", intArray)
}

func stringSliceTest() {
	stringSlice := make([]string, 0)
	for i := 0; i < 10; i++ {
		stringSlice = append(stringSlice, strconv.Itoa(i))
	}
	fmt.Printf("%v", stringSlice)
}

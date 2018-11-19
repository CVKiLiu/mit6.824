package myTest

import "fmt"

func makeArray() {
	intArray := make([]int, 0, 100)
	fmt.Println(len(intArray), " ", cap(intArray))
	intSlice := intArray[:]
	fmt.Println(len(intSlice), " ", cap(intSlice), "")
	fmt.Println(intSlice, " ", intArray)
}

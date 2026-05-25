// Tiny age-keygen helper for the asciinema demo. Prints two lines:
// AGE-SECRET-KEY-... and age1.... record.sh greps each in turn.
package main

import (
	"fmt"

	"filippo.io/age"
)

func main() {
	id, err := age.GenerateX25519Identity()
	if err != nil {
		panic(err)
	}
	fmt.Println("# private:")
	fmt.Println(id.String())
	fmt.Println("# recipient:")
	fmt.Println(id.Recipient().String())
}

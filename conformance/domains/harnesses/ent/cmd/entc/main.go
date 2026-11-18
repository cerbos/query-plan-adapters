package main

import (
	"log"
	"os"

	"entgo.io/ent/entc"
	"entgo.io/ent/entc/gen"

	"github.com/cerbos/query-plan-adapters/ent/entcerbos"
)

func main() {
	for _, d := range os.Args[1:] {
		err := entc.Generate("./schema/"+d, &gen.Config{
			Target:  "./gen/" + d,
			Package: "github.com/cerbos/query-plan-adapters/conformance/domains/harnesses/ent/gen/" + d,
		}, entc.Extensions(entcerbos.Extension{}))
		if err != nil {
			log.Fatalf("%s: %v", d, err)
		}
	}
}

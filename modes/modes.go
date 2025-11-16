package modes

import (
	"flag"
)

type mode struct {
	CreateSqlFilesMode bool
	NewSchemaMode      bool
}

func FileMode() mode {
	mod := mode{}

	// create sql files mode flag -- -c
	createSqlFilesMode := flag.Bool("c", false, "create sql files mode")
	flagValue := flag.Bool("n", false, "use new schema mode")
	flag.Parse()

	if createSqlFilesMode != nil && *createSqlFilesMode {
		mod.CreateSqlFilesMode = true
	}

	if flagValue != nil && *flagValue {
		mod.NewSchemaMode = true
	}

	return mod
}

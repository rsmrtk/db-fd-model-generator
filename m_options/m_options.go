package m_options

import (
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/rsmrtk/db-fd-model-generator/logger"
)

type Options struct {
	Log *logger.Logger
	DB  *spanner.Client
}

func (o Options) IsValid() error {
	if o == (Options{}) {
		return fmt.Errorf("options is empty")
	}
	if o.Log == nil {
		return fmt.Errorf("log is nil")
	}
	if o.DB == nil {
		return fmt.Errorf("db is nil")
	}
	return nil
}

package m_options

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rsmrtk/db-fd-model-generator/logger"
)

type Options struct {
	Log *logger.Logger
	DB  *pgxpool.Pool
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

package consumer

import (
	"context"

	"github.com/stewelarend/logger"
)

type IContext interface {
	context.Context
	logger.ILogger
}

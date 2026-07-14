package daemon

import (
	"context"
	"github.com/amitray007/loco/internal/notification"
	"time"
)

func Notify(ctx context.Context, socket string, timeout time.Duration, input notification.Input) (notification.Result, error) {
	var result notification.Result
	err := eventRequest(ctx, socket, timeout, "notify.send", input, &result)
	return result, err
}

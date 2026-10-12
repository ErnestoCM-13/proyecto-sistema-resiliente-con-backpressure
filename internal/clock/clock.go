// Reads the system monotonic clock
package clock

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func NowNS() (int64, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		return 0, fmt.Errorf("clock_gettime(CLOCK_MONOTONIC): %w", err)
	}
	return ts.Nano(), nil
}

package buildscripts // import "github.com/open-telemetry/opentelemetry-collector-contrib/internal/buildscripts"

import (
	"bytes"
	"fmt"
)

// DedupeChloggenComponentsConfig removes duplicate component entries inside the
// top-level "components" section while preserving first-seen order.
func DedupeChloggenComponentsConfig(input []byte) ([]byte, error) {
	lines := bytes.SplitAfter(input, []byte("\n"))
	if len(lines) == 0 {
		return input, nil
	}

	var output bytes.Buffer
	inComponents := false
	seen := map[string]struct{}{}

	for _, line := range lines {
		trimmedLine := bytes.TrimRight(line, "\r\n")
		switch {
		case bytes.Equal(trimmedLine, []byte("components:")):
			inComponents = true
			output.Write(line)
			continue
		case inComponents && len(trimmedLine) > 0 && trimmedLine[0] != ' ':
			inComponents = false
		}

		if inComponents && bytes.HasPrefix(trimmedLine, []byte("    - ")) {
			key := string(bytes.TrimPrefix(trimmedLine, []byte("    - ")))
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
		}

		output.Write(line)
	}

	if output.Len() == 0 {
		return nil, fmt.Errorf("deduped config is empty")
	}

	return output.Bytes(), nil
}

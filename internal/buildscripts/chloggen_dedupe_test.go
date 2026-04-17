package buildscripts

import "testing"

func TestDedupeChloggenComponentsConfig_RemovesDuplicateComponents(t *testing.T) {
	t.Parallel()

	input := []byte(`components:
  contrib:
    - exporter/foo
    - scraper/system
    - scraper/system
    - scraper/zookeeper
notes:
  keep: true
`)

	output, err := DedupeChloggenComponentsConfig(input)
	if err != nil {
		t.Fatalf("dedupe chloggen config: %v", err)
	}

	want := `components:
  contrib:
    - exporter/foo
    - scraper/system
    - scraper/zookeeper
notes:
  keep: true
`

	if string(output) != want {
		t.Fatalf("unexpected output:\n%s", output)
	}
}

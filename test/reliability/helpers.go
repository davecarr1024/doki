//go:build reliability

package reliability

import (
	"bytes"
	"encoding/json"
	"io"
)

func marshalJSON(v any) (*bytes.Reader, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

func decodeJSON(r io.Reader, out any) error {
	return json.NewDecoder(r).Decode(out)
}

package main

import (
	"encoding/json"

	elastic "gopkg.in/olivere/elastic.v5"
)

// es7CompatibleDecoder wraps the elastic default decoder and transparently
// converts Elasticsearch 7 style hits.total objects into the integer format
// expected by olivere/elastic.v5. This keeps the rest of the codebase
// unchanged while allowing elktail to talk to newer clusters.
type es7CompatibleDecoder struct {
	fallback elastic.Decoder
}

func newES7CompatibleDecoder() elastic.Decoder {
	return &es7CompatibleDecoder{fallback: &elastic.DefaultDecoder{}}
}

func (d *es7CompatibleDecoder) Decode(data []byte, v interface{}) error {
	if err := d.fallback.Decode(data, v); err != nil {
		if patched, changed, patchErr := patchHitsTotalFields(data); patchErr == nil && changed {
			if retryErr := d.fallback.Decode(patched, v); retryErr == nil {
				return nil
			} else {
				return retryErr
			}
		}
		return err
	}
	return nil
}

func patchHitsTotalFields(data []byte) ([]byte, bool, error) {
	var payload interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, false, err
	}

	changed := convertHitsTotals(payload)
	if !changed {
		return nil, false, nil
	}

	patched, err := json.Marshal(payload)
	if err != nil {
		return nil, false, err
	}
	return patched, true, nil
}

func convertHitsTotals(node interface{}) bool {
	switch val := node.(type) {
	case map[string]interface{}:
		changed := false
		if convertHitsTotal(val) {
			changed = true
		}
		for _, child := range val {
			if convertHitsTotals(child) {
				changed = true
			}
		}
		return changed
	case []interface{}:
		changed := false
		for _, child := range val {
			if convertHitsTotals(child) {
				changed = true
			}
		}
		return changed
	default:
		return false
	}
}

func convertHitsTotal(m map[string]interface{}) bool {
	totalMap, ok := m["total"].(map[string]interface{})
	if !ok {
		return false
	}

	rawValue, ok := totalMap["value"]
	if !ok {
		return false
	}

	intVal, ok := toInt64(rawValue)
	if !ok {
		return false
	}

	m["total"] = intVal

	return true
}

func toInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case float64:
		return int64(v), true
	case json.Number:
		if intVal, err := v.Int64(); err == nil {
			return intVal, true
		}
		return 0, false
	default:
		return 0, false
	}
}

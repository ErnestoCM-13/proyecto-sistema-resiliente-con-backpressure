// Defines the Hot Sale order event as it arrives in the JSONL log.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const TimeLayout = "2006-01-02T15:04:05"

type Type string

const (
	OrderPlaced       Type = "order_placed"
	PaymentConfirmed  Type = "payment_confirmed"
	InventoryReserved Type = "inventory_reserved"
)

var Types = [3]Type{OrderPlaced, PaymentConfirmed, InventoryReserved}

func ParseType(s string) (Type, error) {
	for _, t := range Types {
		if Type(s) == t {
			return t, nil
		}
	}
	return "", fmt.Errorf("unknown event_type %q", s)
}

func (t Type) Index() int {
	for i, v := range Types {
		if v == t {
			return i
		}
	}
	return -1
}

func (t *Type) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("event_type: %w", err)
	}
	parsed, err := ParseType(s)
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}

type Country string

const (
	MX Country = "MX"
	AR Country = "AR"
	CO Country = "CO"
	CL Country = "CL"
	PE Country = "PE"
)

var Countries = [5]Country{MX, AR, CO, CL, PE}

func ParseCountry(s string) (Country, error) {
	for _, c := range Countries {
		if Country(s) == c {
			return c, nil
		}
	}
	return "", fmt.Errorf("unknown country %q", s)
}

func (c Country) Index() int {
	for i, v := range Countries {
		if v == c {
			return i
		}
	}
	return -1
}

func (c *Country) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("country: %w", err)
	}
	parsed, err := ParseCountry(s)
	if err != nil {
		return err
	}
	*c = parsed
	return nil
}

type Time struct {
	time.Time
}

func (t *Time) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("timestamp: %w", err)
	}
	parsed, err := time.Parse(TimeLayout, s)
	if err != nil {
		return fmt.Errorf("timestamp: %w", err)
	}
	t.Time = parsed
	return nil
}

type Event struct {
	EventID   string   `json:"event_id"`
	Type      Type     `json:"event_type"`
	OrderID   string   `json:"order_id"`
	Country   Country  `json:"country"`
	AmountUSD *float64 `json:"amount_usd"`
	Timestamp Time     `json:"timestamp"`
}

func (e *Event) UnmarshalJSON(b []byte) error {
	type raw Event

	var r raw
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}

	var problems []error
	if r.EventID == "" {
		problems = append(problems, errors.New("event_id: missing or empty"))
	}
	if r.Type == "" {
		problems = append(problems, errors.New("event_type: missing"))
	}
	if r.OrderID == "" {
		problems = append(problems, errors.New("order_id: missing or empty"))
	}
	if r.Country == "" {
		problems = append(problems, errors.New("country: missing"))
	}
	if r.Timestamp.IsZero() {
		problems = append(problems, errors.New("timestamp: missing"))
	}
	if len(problems) > 0 {
		return errors.Join(problems...)
	}

	*e = Event(r)
	return nil
}

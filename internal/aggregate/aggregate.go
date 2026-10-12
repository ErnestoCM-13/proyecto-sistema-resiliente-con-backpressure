package aggregate

import (
	"math"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/event"
)

type Aggregate struct {
	counts [len(event.Countries)][len(event.Types)]int64
	cents  [len(event.Countries)][len(event.Types)]int64
}

func New() *Aggregate {
	return &Aggregate{}
}

func (a *Aggregate) Add(ev *event.Event) {
	ci, ti := ev.Country.Index(), ev.Type.Index()
	a.counts[ci][ti]++
	if ev.AmountUSD != nil {
		a.cents[ci][ti] += int64(math.Round(*ev.AmountUSD * 100))
	}
}

func (a *Aggregate) Count(country event.Country, evType event.Type) int64 {
	return a.counts[country.Index()][evType.Index()]
}

func (a *Aggregate) Cents(country event.Country, evType event.Type) int64 {
	return a.cents[country.Index()][evType.Index()]
}

func (a *Aggregate) Total() int64 {
	var total int64
	for ci := range a.counts {
		for ti := range a.counts[ci] {
			total += a.counts[ci][ti]
		}
	}
	return total
}

func (a *Aggregate) CountByType(evType event.Type) int64 {
	var total int64
	ti := evType.Index()
	for ci := range a.counts {
		total += a.counts[ci][ti]
	}
	return total
}

func (a *Aggregate) CountByCountry(country event.Country) int64 {
	var total int64
	for _, n := range a.counts[country.Index()] {
		total += n
	}
	return total
}

type Snapshot struct {
	Total           int64                       `json:"total_events"`
	ByType          map[string]int64            `json:"by_type"`
	ByCountry       map[string]int64            `json:"by_country"`
	ByCountryType   map[string]map[string]int64 `json:"by_country_and_type"`
	CentsByCountry  map[string]map[string]int64 `json:"amount_cents_by_country_and_type"`
	AmountUSDByType map[string]float64          `json:"amount_usd_by_type"`
}

func (a *Aggregate) Snapshot() Snapshot {
	snap := Snapshot{
		Total:           a.Total(),
		ByType:          map[string]int64{},
		ByCountry:       map[string]int64{},
		ByCountryType:   map[string]map[string]int64{},
		CentsByCountry:  map[string]map[string]int64{},
		AmountUSDByType: map[string]float64{},
	}
	for _, country := range event.Countries {
		perType := map[string]int64{}
		perTypeCents := map[string]int64{}
		for _, evType := range event.Types {
			perType[string(evType)] = a.Count(country, evType)
			perTypeCents[string(evType)] = a.Cents(country, evType)
		}
		snap.ByCountryType[string(country)] = perType
		snap.CentsByCountry[string(country)] = perTypeCents
		snap.ByCountry[string(country)] = a.CountByCountry(country)
	}
	for _, evType := range event.Types {
		snap.ByType[string(evType)] = a.CountByType(evType)
		var cents int64
		for _, country := range event.Countries {
			cents += a.Cents(country, evType)
		}
		snap.AmountUSDByType[string(evType)] = float64(cents) / 100
	}
	return snap
}

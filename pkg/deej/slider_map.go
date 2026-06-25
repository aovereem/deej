package deej

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/thoas/go-funk"
)

type sliderMap struct {
	m    map[int][]string
	lock sync.Locker
}

func newSliderMap() *sliderMap {
	return &sliderMap{
		m:    make(map[int][]string),
		lock: &sync.Mutex{},
	}
}

// sliderMapFromConfigs builds the slider map from the user and internal mappings. it also
// returns the list of invalid (non-numeric or negative) slider indices found in the user
// config so the caller can warn about them - previously these silently parsed to 0 and
// clobbered slider 0's mapping.
func sliderMapFromConfigs(userMapping map[string][]string, internalMapping map[string][]string) (*sliderMap, []string) {
	resultMap := newSliderMap()
	var invalidUserKeys []string

	// copy targets from user config, ignoring empty values
	for sliderIdxString, targets := range userMapping {
		sliderIdx, err := strconv.Atoi(sliderIdxString)
		if err != nil || sliderIdx < 0 {
			invalidUserKeys = append(invalidUserKeys, sliderIdxString)
			continue
		}

		resultMap.set(sliderIdx, funk.FilterString(targets, func(s string) bool {
			return s != ""
		}))
	}

	// add targets from internal configs, ignoring duplicate or empty values.
	// the internal config is machine-written, so invalid keys are skipped silently
	for sliderIdxString, targets := range internalMapping {
		sliderIdx, err := strconv.Atoi(sliderIdxString)
		if err != nil || sliderIdx < 0 {
			continue
		}

		existingTargets, ok := resultMap.get(sliderIdx)
		if !ok {
			existingTargets = []string{}
		}

		filteredTargets := funk.FilterString(targets, func(s string) bool {
			return (!funk.ContainsString(existingTargets, s)) && s != ""
		})

		existingTargets = append(existingTargets, filteredTargets...)
		resultMap.set(sliderIdx, existingTargets)
	}

	return resultMap, invalidUserKeys
}

func (m *sliderMap) iterate(f func(int, []string)) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for key, value := range m.m {
		f(key, value)
	}
}

func (m *sliderMap) get(key int) ([]string, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	value, ok := m.m[key]
	return value, ok
}

func (m *sliderMap) set(key int, value []string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.m[key] = value
}

func (m *sliderMap) String() string {
	m.lock.Lock()
	defer m.lock.Unlock()

	sliderCount := 0
	targetCount := 0

	for _, value := range m.m {
		sliderCount++
		targetCount += len(value)
	}

	return fmt.Sprintf("<%d sliders mapped to %d targets>", sliderCount, targetCount)
}

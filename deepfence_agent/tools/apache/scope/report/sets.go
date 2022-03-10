package report

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"

	"github.com/ugorji/go/codec"
	"github.com/weaveworks/ps"
)

var customHashMap = &CustomHashmap{
	Map: map[string]interface{}{},
	mu:  sync.RWMutex{},
}

func NewCustomHashMap() *CustomHashmap {
	return customHashMap
}

type CustomHashmap struct {
	Map map[string]interface{}
	mu  sync.RWMutex
}

func (cm *CustomHashmap) IsNil() bool {
	return cm == nil
}

func (cm *CustomHashmap) Set(key string, value interface{}) ps.Map {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.Map[key] = value
	return cm
}

func (cm *CustomHashmap) UnsafeMutableSet(key string, value interface{}) ps.Map {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.Map[key] = value
	return cm
}

func (cm *CustomHashmap) Delete(key string) ps.Map {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.Map, key)
	return cm
}

func (cm *CustomHashmap) Lookup(key string) (interface{}, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if val, ok := cm.Map[key]; ok {
		return val, ok
	}
	return nil, false
}

func (cm *CustomHashmap) Size() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.Map)
}

func (cm *CustomHashmap) ForEach(f func(key string, val interface{})) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	for k, v := range cm.Map {
		f(k, v)
	}
}

func (cm *CustomHashmap) Keys() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	res := []string{}
	for k, _ := range cm.Map {
		res = append(res, k)
	}
	return res
}

func (cm *CustomHashmap) String() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	keys := cm.Keys()
	buf := bytes.NewBufferString("{")
	for _, key := range keys {
		val, _ := cm.Lookup(key)
		fmt.Fprintf(buf, "%s: %s, ", key, val)
	}
	fmt.Fprintf(buf, "}\n")
	return buf.String()
}

// Sets is a string->set-of-strings map.
// It is immutable.
type Sets struct {
	psMap ps.Map
}

// EmptySets is an empty Sets.  Starts with this.
var emptySets = Sets{NewCustomHashMap()}

// MakeSets returns EmptySets
func MakeSets() Sets {
	return emptySets
}

// Keys returns the keys for this set
func (s Sets) Keys() []string {
	if s.psMap == nil {
		return nil
	}
	return s.psMap.Keys()
}

// Add the given value to the Sets.
func (s Sets) Add(key string, value StringSet) Sets {
	if s.psMap == nil {
		s = emptySets
	}
	if existingValue, ok := s.psMap.Lookup(key); ok {
		var unchanged bool
		value, unchanged = existingValue.(StringSet).Merge(value)
		if unchanged {
			return s
		}
	}
	return Sets{
		psMap: s.psMap.Set(key, value),
	}
}

// AddString adds a single string under a key, creating a new StringSet if necessary.
func (s Sets) AddString(key string, str string) Sets {
	if s.psMap == nil {
		s = emptySets
	}
	value, found := s.Lookup(key)
	if found && value.Contains(str) {
		return s
	}
	value = value.Add(str)
	return Sets{
		psMap: s.psMap.Set(key, value),
	}
}

// Delete the given set from the Sets.
func (s Sets) Delete(key string) Sets {
	if s.psMap == nil {
		return emptySets
	}
	psMap := s.psMap.Delete(key)
	if psMap.IsNil() {
		return emptySets
	}
	return Sets{psMap: psMap}
}

// Lookup returns the sets stored under key.
func (s Sets) Lookup(key string) (StringSet, bool) {
	if s.psMap == nil {
		return MakeStringSet(), false
	}
	if value, ok := s.psMap.Lookup(key); ok {
		return value.(StringSet), true
	}
	return MakeStringSet(), false
}

// Size returns the number of elements
func (s Sets) Size() int {
	if s.psMap == nil {
		return 0
	}
	return s.psMap.Size()
}

// Merge merges two sets maps into a fresh set, performing set-union merges as
// appropriate.
func (s Sets) Merge(other Sets) Sets {
	var (
		sSize     = s.Size()
		otherSize = other.Size()
		result    = s.psMap
		iter      = other.psMap
	)
	switch {
	case sSize == 0:
		return other
	case otherSize == 0:
		return s
	case sSize < otherSize:
		result, iter = iter, result
	}

	iter.ForEach(func(key string, value interface{}) {
		set := value.(StringSet)
		if existingSet, ok := result.Lookup(key); ok {
			var unchanged bool
			set, unchanged = existingSet.(StringSet).Merge(set)
			if unchanged {
				return
			}
		}
		result = result.Set(key, set)
	})

	return Sets{result}
}

func (s Sets) String() string {
	return mapToString(s.psMap)
}

// DeepEqual tests equality with other Sets
func (s Sets) DeepEqual(t Sets) bool {
	return mapEqual(s.psMap, t.psMap, reflect.DeepEqual)
}

// CodecEncodeSelf implements codec.Selfer
func (s *Sets) CodecEncodeSelf(encoder *codec.Encoder) {
	mapWrite(s.psMap, encoder, func(encoder *codec.Encoder, val interface{}) {
		encoder.Encode(val.(StringSet))
	})
}

// CodecDecodeSelf implements codec.Selfer
func (s *Sets) CodecDecodeSelf(decoder *codec.Decoder) {
	out := mapRead(decoder, func(isNil bool) interface{} {
		var value StringSet
		if !isNil {
			decoder.Decode(&value)
		}
		return value
	})
	*s = Sets{out}
}

// MarshalJSON shouldn't be used, use CodecEncodeSelf instead
func (Sets) MarshalJSON() ([]byte, error) {
	panic("MarshalJSON shouldn't be used, use CodecEncodeSelf instead")
}

// UnmarshalJSON shouldn't be used, use CodecDecodeSelf instead
func (*Sets) UnmarshalJSON(b []byte) error {
	panic("UnmarshalJSON shouldn't be used, use CodecDecodeSelf instead")
}

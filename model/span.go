// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"encoding/gob"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/opentracing/opentracing-go/ext"
)

const (
	// sampledFlag is the bit set in Flags in order to define a span as a sampled span
	sampledFlag = Flags(1)
	// debugFlag is the bit set in Flags in order to define a span as a debug span
	debugFlag = Flags(2)
)

// // TraceID is a random 128bit identifier for a trace
// type TraceID struct {
// 	Low  uint64 `json:"lo"`
// 	High uint64 `json:"hi"`
// }

// Flags is a bit map of flags for a span
type Flags uint32

// SpanID is a random 64bit identifier for a span
type SpanID uint64

// Span represents a unit of work in an application, such as an RPC, a database call, etc.
// type Span struct {
// 	TraceID       TraceID       `json:"traceID"`
// 	SpanID        SpanID        `json:"spanID"`
// 	ParentSpanID  SpanID        `json:"parentSpanID"`
// 	OperationName string        `json:"operationName"`
// 	References    []SpanRef     `json:"references,omitempty"`
// 	Flags         Flags         `json:"flags,omitempty"`
// 	StartTime     time.Time     `json:"startTime"`
// 	Duration      time.Duration `json:"duration"`
// 	Tags          []KeyValue    `json:"tags,omitempty"`
// 	Logs          []Log         `json:"logs,omitempty"`
// 	Process       *Process      `json:"process"`
// 	Warnings      []string      `json:"warnings,omitempty"`
// }

// Hash implements Hash from Hashable.
func (s *Span) Hash(w io.Writer) (err error) {
	// gob is not the most efficient way, but it ensures we don't miss any fields.
	// See BenchmarkSpanHash in span_test.go
	enc := gob.NewEncoder(w)
	return enc.Encode(s)
}

// HasSpanKind returns true if the span has a `span.kind` tag set to `kind`.
func (s *Span) HasSpanKind(kind ext.SpanKindEnum) bool {
	if tag, ok := KeyValues(s.Tags).FindByKey(string(ext.SpanKind)); ok {
		return tag.AsString() == string(kind)
	}
	return false
}

// IsRPCClient returns true if the span represents a client side of an RPC,
// as indicated by the `span.kind` tag set to `client`.
func (s *Span) IsRPCClient() bool {
	return s.HasSpanKind(ext.SpanKindRPCClientEnum)
}

// IsRPCServer returns true if the span represents a server side of an RPC,
// as indicated by the `span.kind` tag set to `server`.
func (s *Span) IsRPCServer() bool {
	return s.HasSpanKind(ext.SpanKindRPCServerEnum)
}

// NormalizeTimestamps changes all timestamps in this span to UTC.
func (s *Span) NormalizeTimestamps() {
	s.StartTime = s.StartTime.UTC()
	for i := range s.Logs {
		s.Logs[i].Timestamp = s.Logs[i].Timestamp.UTC()
	}
}

// ParentSpanID returns ID of a parent span if it exists.
// It searches for the first child-of reference pointing to the same trace ID.
func (s *Span) ParentSpanID() SpanID {
	for i := range s.References {
		ref := &s.References[i]
		if ref.TraceID == s.TraceID && ref.RefType == ChildOf {
			return ref.SpanID
		}
	}
	return SpanID(0)
}

// ReplaceParentID replaces span ID in the parent span reference.
// See also ParentSpanID.
func (s *Span) ReplaceParentID(newParentID SpanID) {
	oldParentID := s.ParentSpanID()
	for i := range s.References {
		if s.References[i].SpanID == oldParentID && s.References[i].TraceID == s.TraceID {
			s.References[i].SpanID = newParentID
			return
		}
	}
	s.References = MaybeAddParentSpanID(s.TraceID, newParentID, s.References)
}

// ------- Flags -------

// SetSampled sets the Flags as sampled
func (f *Flags) SetSampled() {
	f.setFlags(sampledFlag)
}

// SetDebug set the Flags as sampled
func (f *Flags) SetDebug() {
	f.setFlags(debugFlag)
}

func (f *Flags) setFlags(bit Flags) {
	*f = *f | bit
}

// IsSampled returns true if the Flags denote sampling
func (f Flags) IsSampled() bool {
	return f.checkFlags(sampledFlag)
}

// IsDebug returns true if the Flags denote debugging
// Debugging can be useful in testing tracing availability or correctness
func (f Flags) IsDebug() bool {
	return f.checkFlags(debugFlag)
}

func (f Flags) checkFlags(bit Flags) bool {
	return f&bit == bit
}

// ------- TraceID -------

// String renders trace id as a single hex string.
func (t TraceID) String() string {
	if t.High == 0 {
		return fmt.Sprintf("%x", t.Low)
	}
	return fmt.Sprintf("%x%016x", t.High, t.Low)
}

// TraceIDFromString creates a TraceID from a hexadecimal string
func TraceIDFromString(s string) (TraceID, error) {
	var hi, lo uint64
	var err error
	if len(s) > 32 {
		return TraceID{}, fmt.Errorf("TraceID cannot be longer than 32 hex characters: %s", s)
	} else if len(s) > 16 {
		hiLen := len(s) - 16
		if hi, err = strconv.ParseUint(s[0:hiLen], 16, 64); err != nil {
			return TraceID{}, err
		}
		if lo, err = strconv.ParseUint(s[hiLen:], 16, 64); err != nil {
			return TraceID{}, err
		}
	} else {
		if lo, err = strconv.ParseUint(s, 16, 64); err != nil {
			return TraceID{}, err
		}
	}
	return TraceID{High: hi, Low: lo}, nil
}

// MarshalJSONPB renders trace id as a single hex string.
func (t TraceID) MarshalJSONPB(*jsonpb.Marshaler) ([]byte, error) {
	var b strings.Builder
	s := t.String()
	b.Grow(2 + len(s))
	b.WriteByte('"')
	b.WriteString(s)
	b.WriteByte('"')
	return []byte(b.String()), nil
}

// UnmarshalJSONPB TODO
func (t *TraceID) UnmarshalJSONPB(_ *jsonpb.Unmarshaler, b []byte) error {
	if len(b) < 3 {
		return fmt.Errorf("TraceID JSON string cannot be shorter than 3 chars: %s", string(b))
	}
	if b[0] != '"' || b[len(b)-1] != '"' {
		return fmt.Errorf("TraceID JSON string must be enclosed in quotes: %s", string(b))
	}
	q, err := TraceIDFromString(string(b[1 : len(b)-1]))
	if err != nil {
		return err
	}
	*t = q
	return nil
}

// MarshalText is called by encoding/json, which we do not want people to use.
func (t TraceID) MarshalText() ([]byte, error) {
	return nil, fmt.Errorf("unsupported method TraceID.MarshalText; please use github.com/gogo/protobuf/jsonpb for marshalling")
}

// UnmarshalText is called by encoding/json, which we do not want people to use.
func (t *TraceID) UnmarshalText(text []byte) error {
	return fmt.Errorf("unsupported method TraceID.UnmarshalText; please use github.com/gogo/protobuf/jsonpb for marshalling")
}

// ------- SpanID -------

// String converts SpanID to a hex string.
func (s SpanID) String() string {
	return fmt.Sprintf("%x", uint64(s))
}

// SpanIDFromString creates a SpanID from a hexadecimal string
func SpanIDFromString(s string) (SpanID, error) {
	if len(s) > 16 {
		return SpanID(0), fmt.Errorf("SpanID cannot be longer than 16 hex characters: %s", s)
	}
	id, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return SpanID(0), err
	}
	return SpanID(id), nil
}

// MarshalJSONPB renders span id as a single hex string.
// TODO this method is never called by "github.com/gogo/protobuf/jsonpb" Marshaler.
func (s SpanID) MarshalJSONPB(*jsonpb.Marshaler) ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, s.String())), nil
}

// MarshalText allows SpanID to serialize itself in JSON as a string.
func (s SpanID) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// UnmarshalJSONPB TODO
func (s *SpanID) UnmarshalJSONPB(_ *jsonpb.Unmarshaler, b []byte) error {
	if len(b) < 3 {
		return fmt.Errorf("SpanID JSON string cannot be shorter than 3 chars: %s", string(b))
	}
	if b[0] != '"' || b[len(b)-1] != '"' {
		return fmt.Errorf("SpanID JSON string must be enclosed in quotes: %s", string(b))
	}
	q, err := SpanIDFromString(string(b[1 : len(b)-1]))
	if err != nil {
		return err
	}
	*s = q
	return nil
}

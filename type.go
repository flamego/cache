// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package cache

import (
	"bytes"
	"encoding/gob"
)

// Encoder is an encoder to encode cache data to binary.
type Encoder func(interface{}) ([]byte, error)

// Decoder is a decoder to decode binary to cache data.
type Decoder func([]byte) (interface{}, error)

// GobEncoder is a cache data encoder using Gob.
func GobEncoder(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GobDecoder is a cache data decoder using Gob.
func GobDecoder(binary []byte) (interface{}, error) {
	buf := bytes.NewBuffer(binary)
	var v interface{}
	return v, gob.NewDecoder(buf).Decode(&v)
}

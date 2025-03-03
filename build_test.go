package rct

import (
	"slices"
	"testing"
)

type builderTestCase struct {
	Dg     Datagram
	Expect []byte
}

var builderTestCases = []builderTestCase{
	{Datagram{Read, BatteryPowerW, nil}, []byte{0x2B, 0x01, 0x04, 0x40, 0x0F, 0x01, 0x5B, 0x58, 0xB4}},
	{Datagram{Read, InverterACPowerW, nil}, []byte{0x2B, 0x01, 0x04, 0xDB, 0x2D, 0x2D, 0x69, 0xAE, 0x55, 0xAB}},
}

// Test if builder returns expected byte representation
func TestBuilder(t *testing.T) {
	builder := new(DatagramBuilder)
	for _, tc := range builderTestCases {
		builder.Build(&tc.Dg)
		res := builder.Bytes()
		if slices.Compare(res, tc.Expect) != 0 {
			t.Errorf("error got %s, should be %s", res, tc.Expect)
		}
	}
}

// Test if roundtrip from builder to parser returns the same datagram
func TestBuilderParser(t *testing.T) {
	builder := new(DatagramBuilder)
	parser := NewDatagramParser()

	for _, tc := range builderTestCases {
		builder.Build(&tc.Dg)
		parser.Reset()
		parser.buffer = builder.Bytes()
		parser.length = len(builder.Bytes())
		dg, err := parser.Parse()
		if err != nil {
			t.Errorf("%s", err.Error())
		}
		if dg.Cmd != tc.Dg.Cmd || dg.Id != tc.Dg.Id || len(dg.Data) != len(tc.Dg.Data) {
			t.Errorf("error mismatch got %s, expect %s", dg.String(), tc.Dg.String())
		}
		for i := 0; i < len(dg.Data); i++ {
			if dg.Data[i] != tc.Dg.Data[i] {
				t.Errorf("error mismatch got %s, expect %s", dg.String(), tc.Dg.String())
			}
		}
	}
}

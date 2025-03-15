# rct

[![Go Report Card](https://goreportcard.com/badge/github.com/evcc-io/rct?style=flat-square)](https://goreportcard.com/report/github.com/evcc-io/rct)
[![PkgGoDev](https://pkg.go.dev/badge/mod/github.com/evcc-io/rct)](https://pkg.go.dev/mod/github.com/evcc-io/rct)

A library for communication with solar power inverters of the RCT power brand.
Tested with the RCT PS 6.0 solar power inverter, battery and grid power sensor.

RCT power is a registered trademark of RCT Power GmbH. This library is not provided by, endorsed by, supported by or affiliated with the company in any way.

It is provided without any warranties, entirely for use at your own risk under a LGPL 2.1 license.

## Usage

Install via `go get github.com/evcc-io/rct`.

See https://github.com/evcc-io/evcc/blob/master/meter/rct.go for an example usage.

## Architecture

- `datagram.go` defines basic constants like commands, on-device identifiers and datagram packets; as well as conversions of datagram payloads to golang types
- `crc.go` defines the cyclic redundancy check algorithm to ensure data integrity used by the RCT
- `build.go` defines a datagram builder for assembling datagrams to send
- `parse.go` defines a datagram parser which parses incoming bytes into datagrams
- `connection.go` ties builders and parsers into a bidirectional connection with the device, and defines convenience methods to synchronously query identifiers
- `write.go` defines methods to validate and write data

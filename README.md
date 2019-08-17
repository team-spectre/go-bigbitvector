# go-bigbitvector

A bitvector implementation in Go that can spill to disk.

[![License](https://img.shields.io/github/license/team-spectre/go-bigbitvector.svg?maxAge=86400)](https://github.com/team-spectre/go-bigbitvector/blob/master/LICENSE)
[![GoDoc](https://godoc.org/github.com/team-spectre/go-bigbitvector?status.svg)](https://godoc.org/github.com/team-spectre/go-bigbitvector)
[![Build Status](https://img.shields.io/travis/com/team-spectre/go-bigbitvector.svg?maxAge=3600&logo=travis)](https://travis-ci.com/team-spectre/go-bigbitvector)
[![Issues](https://img.shields.io/github/issues/team-spectre/go-bigbitvector.svg?maxAge=7200&logo=github)](https://github.com/team-spectre/go-bigbitvector/issues)
[![Pull Requests](https://img.shields.io/github/issues-pr/team-spectre/go-bigbitvector.svg?maxAge=7200&logo=github)](https://github.com/team-spectre/go-bigbitvector/pulls)
[![Latest Release](https://img.shields.io/github/release/team-spectre/go-bigbitvector.svg?maxAge=2592000&logo=github)](https://github.com/team-spectre/go-bigbitvector/releases)

This module is intended to be useful for data sets in the range of megabytes
to terabytes: too large to fit in RAM, but small enough to fit on a single
machine, with a strong preference for sequential access patterns.

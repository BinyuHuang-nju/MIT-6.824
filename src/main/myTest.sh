#!/bin/bash

rm mr-*
go build -buildmode=plugin ../mrapps/wc.go

go run mrcoordinator.go pg-*.txt &

go run mrworker.go wc.so &
go run mrworker.go wc.so &
go run mrworker.go wc.so &

wait


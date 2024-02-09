#!/bin/bash

for i in {1..50}; do
    go test > output.txt
    if grep -q "FAIL" output.txt; then
        echo "Error: Test failed on iteration $i!"
        exit 1
    fi
done
#!/bin/bash

# build and deploy adaptive-iterations to wally

cd experiments/ || exit 1
mvn clean deploy -Pdev || exit 1
cd adaptive-iterations-bundle/target/adaptive-iterations/ || exit 1
./peel.sh rsync:push wally || exit 1

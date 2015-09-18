#!/bin/bash

# build and deploy adaptive-flink as flink-0.9.0 to the bundle-bin

# assumes $PEEL_BIN_SYSTEMS exists and points to the $BUNDLE_BIN/adaptive-iterations's 
# systems folder, e.g. $BUNDLE_BIN/adaptive-iterations/systems if not otherwise set
# in the host's application.conf

if [ -z "$PEEL_BIN_SYSTEM" ]; then
    echo "variable PEEL_BIN_SYSTEMS has not been set"
else
    cd adaptive-flink
    mvn clean install -DskipTests
    rm -r $PEEL_BIN_SYSTEMS/flink-0.9.0
    mv build-target/ $PEEL_BIN_SYSTEMS/flink-0.9.0
fi

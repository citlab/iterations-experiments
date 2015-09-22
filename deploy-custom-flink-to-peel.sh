#!/bin/bash

# build and deploy adaptive-flink as flink-0.9.0 to the bundle-bin

# assumes $PEEL_BIN_SYSTEMS exists and points to the $BUNDLE_BIN/adaptive-iterations's 
# systems folder, e.g. $BUNDLE_BIN/adaptive-iterations/systems if not otherwise set
# in the host's application.conf

# note that both the systems folders need to exist (i.e. rsync:push peel bundle first,
# )

cd adaptive-flink
mvn clean install -DskipTests

mv build-target flink-0.9.0

if [ -z "$LOCAL_PEEL_SYSTEMS" ]; then
    echo "variable LOCAL_PEEL_SYSTEMS has not been set"
    echo "should be /PATH/TO/LOCAL/PEEL/BUNDLE/SYSTEMS"
    echo "e.g. /home/[user]/bin/peel/adaptive-iterations/system"
else
    rm -rf $LOCAL_PEEL_SYSTEMS/flink-0.9.0
    cp -R flink-0.9.0 $LOCAL_PEEL_SYSTEMS/flink-0.9.0
fi

if [ -z "$WALLY_PEEL_SYSTEMS" ]; then
    echo "variable WALLY_PEEL_SYSTEMS has not been set"
    echo "should be e.g. [user@]wally-master.cit.tu-berlin.de:/home/[user]/experiments/adaptive-iterations/systems"
else
    scp -r flink-0.9.0 $WALLY_PEEL_SYSTEMS
fi

mv flink-0.9.0 build-target

#!/bin/bash

# build and deploy custom-flink as flink-1.0.0 to the bundle-bin

# assumes $PEEL_BIN_SYSTEMS exists and points to the $BUNDLE_BIN/adaptive-iterations's 
# systems folder, e.g. $BUNDLE_BIN/adaptive-iterations/systems if not otherwise set
# in the host's application.conf

# note that both the systems folders need to exist (i.e. rsync:push bundle first)p

FLINK_VERSION="1.0.0"
FLINK_FOLDER="flink-$FLINK_VERSION"

cd custom-flink
mvn clean install -DskipTests

mv build-target $FLINK_FOLDER

# copy build to local peel systems
if [ -z "$LOCAL_PEEL_SYSTEMS" ]; then
    echo "!! variable LOCAL_PEEL_SYSTEMS has not been set"
    echo "(should be /PATH/TO/LOCAL/PEEL/BUNDLE/SYSTEMS)"
    echo "e.g. /home/[user]/bin/peel/adaptive-iterations/system"
else
    echo ">> Copying flink build to $LOCAL_PEEL_SYSTEMS/$FLINK_FOLDER"
    rm -rf $LOCAL_PEEL_SYSTEMS/$FLINK_FOLDER
    cp -R $FLINK_FOLDER $LOCAL_PEEL_SYSTEMS/$FLINK_FOLDER
fi

# copy build to peel systems on wally
if [ -z "$WALLY_PEEL_SYSTEMS" ]; then
    echo "!! variable WALLY_PEEL_SYSTEMS has not been set"
    echo "(should be e.g. [user@]wally-master.cit.tu-berlin.de:/home/[user]/experiments/adaptive-iterations/systems)"
else
    echo ">> Copying flink build to $WALLY_PEEL_SYSTEMS/$FLINK_FOLDER"
    scp -r $FLINK_FOLDER $WALLY_PEEL_SYSTEMS
fi

mv $FLINK_FOLDER build-target

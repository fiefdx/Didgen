#!/bin/bash

export PATHTOP=$(pwd)
export PATHROOT=${PATHTOP/\/src\/Didgen/}
export GOBIN=$PATHTOP
export GOPATH="$PATHROOT:$PATHTOP/lib"
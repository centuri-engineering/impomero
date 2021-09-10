#! /usr/bin/sh

set -eu


cd server
export OMERO_SERVER=localhost
export OMERO_PORT=4064
export OMERO_ROOT_PASSWORD=omero
export IMPOMERO_DB=impomero.sql


source venv3/bin/activate
python -m impomero -l impomero/data/raw/ &
# PID1=$!
# cp impomero/data/tomls/file0.toml impomero/data/raw/dir0
# sleep 10
# kill $PID1

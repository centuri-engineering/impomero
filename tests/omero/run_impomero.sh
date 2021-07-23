#! /usr/bin/sh

set -eu


cd server
export OMERO_SERVER=localhost
export OMERO_PORT=4064
export OMERO_ROOT_PASSWORD=omero
export IMPOMERO_DB=impomero.sql


source venv3/bin/activate
python -m impomero -l impomero/data/raw/

#! /usr/bin/sh

set -eu


source server/venv3/bin/activate
omero login roo@localhost:4064 -w omero

omero group add Beatles
omero user add john John Lennon --group-name Beatles -P nhoj
omero user add paul Paul McCartney --group-name Beatles -P luap
omero user add george George Harisson --group-name Beatles -P egroeg

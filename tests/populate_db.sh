#! /usr/bin/sh


# the script is run by pytest on the root of the project tree
# hence the data paths
set -eu

omero login root@0.0.0.0:14064 -w omero
omero group add Beatles
omero user add john John Lennon --group-name Beatles -P nhoj
omero user add paul Paul McCartney --group-name Beatles -P luap
omero user add george George Harisson --group-name Beatles -P egroeg
omero import \
      -s 0.0.0.0 -p 14064 \
      -T Project:name:Test_Proj/Dataset:name:Test_Dset \
      --sudo root -w omero -u john \
      data/raw/dir0/sub_dir1/img0.tif

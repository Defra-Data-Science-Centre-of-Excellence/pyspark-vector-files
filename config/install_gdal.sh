#!/bin/bash

# Clear package state information
sudo rm -rf /var/lib/apt/lists/*

# Add UbuntuGIS unstable Private Package Archive (PPA) to access
# more recent version of gdal on Ubuntu 20.04 LTS
# See https://launchpad.net/~ubuntugis/+archive/ubuntu/ubuntugis-unstable
sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable
sudo apt-get update

# The `libgdal-dev` version number must be the same as the Python GDAL bindings
sudo apt-get install -y libgdal-dev=3.4.0+dfsg-1~focal0

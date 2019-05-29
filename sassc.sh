#!/usr/bin/env bash

cd /opt
git clone https://github.com/sass/libsass.git
git clone https://github.com/sass/sassc.git libsass/sassc
export SASS_LIBSASS_PATH=/opt/libsass
cd libsass/sassc
make
ln -s /opt/libsass/sassc/bin/sassc ~/bin/sassc
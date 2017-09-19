#!/bin/bash

# we need pip
which pip || sudo easy_install pip
which virtualenv || sudo pip install virtualenv

if [ ! -e indigoenv ] ; then
    virtualenv -p /usr/bin/python indigoenv
    source indigoenv/bin/activate
    pip install --upgrade pip
    pip install influxdb
fi
[ -e indigoenv ] && source indigoenv/bin/activate

python send_to_influx.py

function undo_source_actions() {
    echo "Turning off virtualenv"
    deactivate
}

undo_source_actions



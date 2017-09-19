echo "Setting up this virtualenv"
# http://python-guide-pt-br.readthedocs.io/en/latest/dev/virtualenvs/

if [ ! -e indigoenv ] ; then 
    virtualenv -p /usr/bin/python indigoenv
    source indigoenv/bin/activate
    pip install --upgrade pip
    pip install influxdb
fi
[ -e indigoenv ] && source indigoenv/bin/activate

function undo_source_actions() {
    echo "Turning off virtualenv"
    deactivate
}



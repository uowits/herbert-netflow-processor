#!/bin/bash

exec 2>&1 >> deploy.log

export PATH="$PATH:/usr/local/bin"
export HOME="$PWD"
umask 022

if test ! -d env
then
	virtualenv env
fi

# install plapi if it's not already
if test ! -f env/lib/python2.7/site-packages/plapi-9.0.0-py2.7-linux-x86_64.egg
then
	./env/bin/easy_install http://download.proceranetworks.com/pldownload/python/plapi/9.0.0/plapi-9.0.0-py2.7-linux-x86_64.egg
fi

./env/bin/pip install -r requirements.txt

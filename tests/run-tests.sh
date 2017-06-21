#!/bin/bash

sitepack=$(python -c 'import sys; \
           sitedirs=[p for p in sys.path if p.endswith("site-packages")]; \
           print sitedirs[0]')

if [[ ! -L $sitepack/argo_egi_connectors ]]
then
    ln -s $PWD/modules $sitepack/argo_egi_connectors
fi

(cd bin && \
for f in [a-z]*.py
do
    link=$(echo $f | sed "s/-/_/g") 
    if [[ ! -L $link ]]
    then
        ln -s $f $link
    fi
done && \
if [[ ! -e __init__.py ]]
then 
    touch __init__.py
fi)

coverage run --source=tests -m unittest2 discover tests && coverage xml  

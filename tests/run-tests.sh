#!/bin/bash

sitepack=$(python -c 'import sys; \
           sitedirs=[p for p in sys.path if p.endswith("site-packages")]; \
           print sitedirs[0]')

declare -a files

if [[ ! -L $sitepack/argo_egi_connectors ]]
then
    ln -s $PWD/modules $sitepack/argo_egi_connectors
    files[${#files[@]}]="$sitepack/argo_egi_connectors"
fi

pushd bin/ > /dev/null

for f in [a-z]*.py
do
    link=${f//-/_} 
    if [[ ! -L $link ]]
    then
        ln -s $f $link
        files[${#files[@]}]="bin/$link"
    fi
done

if [[ ! -e __init__.py ]]
then 
    touch __init__.py
    files[${#files[@]}]="bin/__init__.py"
fi

popd > /dev/null

coverage run --source=tests -m unittest2 discover tests -v && coverage xml

for f in ${files[@]}
do
    rm -f $f*
done

#!/bin/bash

sitepack=$(python -c 'import sys; \
           sitedirs=[p for p in sys.path if p.endswith("site-packages")]; \
           print sitedirs[0]')

[[ ! -L $sitepack/argo_egi_connectors ]] && \
    ln -s $PWD/modules $sitepack/argo_egi_connectors

coverage run --source=tests -m unittest2 discover tests && coverage xml  

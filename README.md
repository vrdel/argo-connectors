**argo-egi-connectors** is a bundle of connectors/sync components for various data sources established in EGI infrastructure, most notably GOCDB (topology, downtimes), but there's also support for alternative topology fetching for virtual organizations, weights factors provided within a VAPOR service and POEM profiles information.

Data is synced on a daily basis and all together with a prefiltered raw metric results coming from _argo-egi-consumer_ represents an input for _argo-compute-engine_. Data is written in binary avro formatted files which are grouped into a directory structure that represents set of customers and jobs at compute side.

Configuration of connectors is centered around two configuration files:
- `global.conf` - used for defining some common information like connection and authentication parameters, avro schemas and output filenames
- `customer.conf` - used for listing jobs for EGI customer and defining new customers and theirs jobs residing in EGI infrastructure. Customers and jobs have attributes that control the behaviour of related connector and also create implicit directory structure. 

More info: http://argoeu.github.io/guides/sync/

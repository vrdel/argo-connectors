---
title: EGI connectors | ARGO
page_title: EGI connectors
font_title: 'fa fa-refresh'
description: This document describes the available connectors for data in EGI infrastructure.
---

## Description

`argo-egi-connectors` is a bundle of connectors/sync components for various data sources established in EGI infrastructure, most notably GOCDB (EGI topology, downtimes), but there's also support for fetching weights information via VAPOR service and POEM metric profiles.

Bundle consists of the following connectors: 

 - `topology-gocdb-connector.py` 
 - `downtimes-gocdb-connector.py` 
 - `weights-vapor-connector.py` 
 - `poem-connector.py`
 - `prefilter-egy.py`: component whose role is to filter out the messages coming from the `argo-egi-consumer`.


Connectors are syncing data on a daily basis. They are aware of the certain customer, associated jobs and their attributes and are generating appropriate data for each job.Data is presented in a form of avro serialized files that are placed in job folders or can be sent to AMS service. Topology, downtimes, weights and POEM profile information all together with a metric results (status messages), represents an input for `argo-compute-engine`.

## Installation

Installation narrows down to simply installing the package:
	
	yum -y install argo-egi-connectors

**`Components require avro, argo-ams-library and pyOpenSSL packages to be installed/available.`**


| File Types | Destination |
| Configuration files|  `/etc/argo-egi-connectors`|
| Components|  `/usr/libexec/argo-egi-connectors`|
| Cronjobs (configured to be executed once per day) | `/etc/cron.d` |
| Directory where components will put their files| `/var/lib/argo-connectors/EGI`|

## Configuration

Configuration of all components is centered around two configuration files: `global.conf` and `customer.conf`. Those file contains some shared config options and sections and are _read by every connector_. There's also a third one `poem-connector.conf`, specific only for `poem-connector.py` because it needs some special treatment not available in first two's.

| Configuration file | Description | Shortcut |
| `global.conf` | Config file consists of global options common to all connectors like the FQDN of AMS service, the path of host certificate to authenticate to a peer, path of correct avro schema and some connection settings like timeout and number of retries. |<a href="#sync1">Description</a>|
| `customer.conf` | This configuration file is specific for each customer and it consists of listed jobs and their attributes | <a href="#sync2">Description</a>|
| `poem-connector.conf` | This configuration file is central configuration for poem-connector.py | <a href="#sync3">Description</a>|

All configuration files reside in `/etc/argo-egi-connectors/` after the installation of the package. That's the default location that each component will try to read configuration from. Location can be overridden since every component takes `-c` and `-g` arguments to explicitly define the paths to `customer.conf` and `global.conf`, respectively. Example:

	topology-gocdb-connector.py -c /path/to/customer-foo.conf -g /path/to/global-foo.conf

Exception is `prefilter-egi.py` component which is interested only in `global.conf` so it takes only `-g` argument.

<a id="sync1"></a>

### global.conf

Config file is read by _every_ component because every component needs to, at least, fetch host certificate to authenticate to a peer, find correct avro schema, know the FQDN of AMS service and have connection parameter properly configured. Config options are case insensitive and whole config file is splitted into a few sections:

	[DEFAULT]
	SchemaDir = /etc/argo-egi-connectors/schemas/
	EGIDir = /var/lib/argo-connectors/EGI

Section contains options that will be combined with others mainly to circumvent the inconvenience of listing their values multiple times along the configuration. Every component generates output file in an avro binary format. `SchemaDir` option points to a directory that holds all avro schemas. `EGIDir` is needed for `prefilter-egi.py` component and must be consistent with the one specified in `customer.conf` for EGI customer.

	[General]
	PublishAms = True
	WriteAvro = True

This section currently has two configuration options affecting the type of delivering the output that each connector generates, so all connectors can write avro encoded data to a files or send the same avro encoded data to AMS service. At least one type of delivering the output data must be enabled. 

	[AMS]
	Host = messaging-devel.argo.grnet.gr
	Token = EGIKEY
	Project = EGI
	Topic = TOPIC
	Bulk = 100  

Section configures parameters needed for AMS service. These are the complete options needed. Some options can be shared across all customers and some like a `Token` and `Project` can be private to each customer so those options can be specified in `[CUSTOMER_*]` section of related `customer.conf`. Splitting of listed options throughout two configuration files `global.conf` and `customer.conf` works as long as the complete set of options is specified so if `Host`, `Token` and `Bulk` are specified in `global.conf`, then `AmsProject` and `AmsToken` should be specified in `customer.conf`.

	[Authentication]
	VerifyServerCert = False
	CAPAth = /etc/grid-security/certificates
	HostKey = /etc/grid-security/hostkey.pem
	HostCert = /etc/grid-security/hostcert.pem

Each component that talks to GOCDB or POEM peer authenticates itself with a host certificate. `HostKey` indicates the private and `HostCert` indicates the public part of certificate. Additionally, server certificate can be validated with the help of `pyOpenSSL` rounding up the mutual authentication. `CAPath` contains certificates of authorities from which chain will be tried to be built upon validating.

	[Connection]
	Timeout = 180
	Retry = 3

For every connector, connection will timeout after `180` seconds specified in `Timeout` by default, if peer doesn't respond properly in a given time frame. Connection will try to be established `3` more times (specified in `Retry`) before connector considers peer unavailable.

	[Prefilter]
	ConsumerFilePath = /var/lib/argo-egi-consumer/argo-consumer_log_DATE.avro
	PoemNameMapping = poem_name_mapping.cfg
	PoemExpandedProfiles = %(EGIDir)s/poem_sync_DATE.out
	LookbackPoemExpandedProfiles = 5

Section defines parameters needed for `prefilter-egi.py` component. It's not connector like rest of the components and its purpose is to filter `ConsumerFilePath` messages based on criteria found in file specified by `PoemExpandProfiles` option so those two options represents its input. If `PoemExpandedProfiles` file is not found, it will try to look for it for `LookbackPoemExpandedProfiles` previous days.

	[AvroSchemas]
	Downtimes = %(SchemaDir)s/downtimes.avsc
	Poem = %(SchemaDir)s/metric_profiles.avsc
	Prefilter = %(SchemaDir)s/metric_data.avsc
	TopologyGroupOfEndpoints = %(SchemaDir)s/group_endpoints.avsc
	TopologyGroupOfGroups = %(SchemaDir)s/group_groups.avsc
	Weights = %(SchemaDir)s/weight_sites.avsc

This section, together with a `[DEFAULT]` section, constitutes the full path of avro schema file for each component. Avro schema files define the format of the data that each component is writing. `Topology*` schemas are common to `topology-gocdb-connector.py`. `Prefilter` schema is the same as of the schema of metric results.

	[Output]
	Downtimes = downtimes_DATE.avro
	Poem = poem_sync_DATE.avro
	Prefilter = %(EGIDir)s/prefilter_DATE.avro
	TopologyGroupOfEndpoints = group_endpoints_DATE.avro
	TopologyGroupOfGroups = group_groups_DATE.avro
	Weights = weights_DATE.avro

Section lists all the filenames that each component is generating. Directory is purposely omitted because it's implicitly found in next configuration file. Exception is `Prefilter` that has its output directory explicitly specified. `DATE` is a string placeholder that will be replaced by the date timestamp in format `year_month_day`.

<a id="sync2"></a>

### customer.conf

This configuration file lists all customers, their jobs and appropriate attributes. Customer in an EGI infrastructure could be a specific VO or even a NGI. Job is presented to `argo-compute-engine` as a folder with a set of files that are generated each day and that directs compute engine what metric results to take into account and do calculations upon them.

#### Directory structure

Job folders for each customer are placed under the customer's `OutputDir` directory and appropriate directory names are read from the config file. Segment of configuration file that reflects the creation of directories is for example: 

	[CUSTOMER_C1]
	Name = C1Name1
	OutputDir = /var/lib/argo-connectors/Customer1
	Jobs = JOB_Test1, JOB_Test2

	[JOB_Test1]
	Dirname = C1Testing1

	[JOB_Test2]
	Dirname = C1Testing2


	[CUSTOMER_C2]
	Name = C2Name2
	OutputDir = /var/lib/argo-connectors/Customer2
	Jobs = Job_Test3, JOB_Test4

	[JOB_Test3]
	Dirname = C2Testing1

	[JOB_Test4]
	Dirname = C2Testing2

This will result in the following jobs directories:

	/var/lib/argo-connectors/Customer1/C1Testing1
	/var/lib/argo-connectors/Customer1/C1Testing2
	/var/lib/argo-connectors/Customer2/C2Testing1
	/var/lib/argo-connectors/Customer2/C2Testing2

So there are two customers, C1 and C2, each one identified with its `[CUSTOMER_*]` section. `CUSTOMER_` is a section keyword and must be specified when one wants to define a new customer. Each customer has three mandatory options: `Name`, `OutputDir` and `Jobs`. With `OutputDir` option, customer defines his directory where he'll write job folders and other data. Customer must also specify set of jobs listed in `Jobs` options since it can not exist without associated jobs. The name of the job folder is specified with `Dirname` option of the certain job so `JOB\_Test1`, identified with `[JOB_Test1]` section, will be named `C1Testing1` and it will be placed under customer's `/var/lib/argo-connectors/Customer1/` directory. Each component will firstly try to create described directory structure if it doesn't exist yet. Only afterward it will write its data.

Every connector reads this configuration file because it needs to find out how many customers are there and what are theirs customer and job directory names where they will lay down its files. So `poem-connector.py`, `downtimes-gocdb-connector.py`, `weights-vapor-connector.py` and `topology-gocdb-connector.py`, all of them are writing theirs data in each job directory for each customer. In case of `PublishAms`, appropriate attribute value taken from `Dirname` will be generated for each AMS message designating to which job it applies and then it will be dispatched to AMS service.

#### Job attributes

Besides `Dirname` option that is common for all connectors, some of them have job attributes that are relevant only for them and upon which they are changing their behaviour. Some of those attributes are _mandatory_ like `Profiles` and the other ones like `TopoSelect*` attributes are optional.

##### GOCDB topology

	[JOB_EGICloudmon]
	Dirname = EGI_Cloudmon
	Profiles = CLOUD-MON
	TopoFeed = https://goc.egi.eu/gocdbpi/
	TopoFetchType = ServiceGroups
	TopoSelectGroupOfEndpoints = Monitored:Y, Scope:EGI, Production:Y
	TopoSelectGroupOfGroups = Monitored:Y, Scope:EGI

This is an example of the job that fetchs topology from GOCDB. `Profiles` is an attribute relevant to `poem-connector.py` so for this job `poem-connector.py` will write CLOUD-MON profile in EGI_Cloudmon job folder under /EGI directory. `Topo*` attributes are relevant for `topology-gocdb-connector.py`. `TopoFeed` attribute in the context of the GOCDB topology is optional. If it's specified, it will override default source of topology which is https://goc.egi.eu/gocdbpi/

Topology is separated in two abstracts:

- group of groups
- group of service endpoints

Service endpoints are grouped either by the means of _Sites_ or _Service groups_. Those are listed and represented as an upper level abstract of group of service endpoints - group of groups. Customer can fetch either _Sites_ and their corresponding endpoints or _Service groups_ and their corresponding endpoints per job, but not both of them. What is being fetched is specified with `TopoFetchType` option/job attribute. For each abstract there will be written `TopologyGroupOfGroups` and `TopologyGroupOfEndpoints` filenames (specified in `global.conf`) into appropriate job folder. `TopoSelectGroupOfGroups` and `TopoSelectGroupOfEndpoints` options are used for further filtering. Values are set of tags used for picking up matching entity existing in the given abstract. Tags for group of groups are different for Sites and Service groups. In contrary, set of tags for groups of endpoints remains the same no matter what type of fetch customer specified.

So, in a `TopoFetchType` option customer can either specify:

- `ServiceGroups` - to fetch Service groups
- `Sites` - to fetch Sites

###### Tags

Tags represent a fine-grained control of what is being written in output files. It's a convenient way of selecting only certain entities, being it Sites, Service groups or Service endpoints based on appropriate criteria. Tags are optional so if a certain tag for a corresponding entity is omitted, than filtering is not done. In that case, it can be considered that entity is fetched for all its values of an omitted tag.

Group of group tags are different for a different type of fetch. Tags and values for a different entities existing in EGI infrastructure are:

**Sites**

* Certification = `{Certified, Uncertified, Closed, Suspended, Candidate}`
* Infrastructure = `{Production, Test}`
* Scope = `{EGI, Local}`

**ServiceGroups**

* Monitored = `{Y, N}`
* Scope = `{EGI, Local}`

Tags for selecting group of endpoints are:

**Service Endpoints**

* Production = `{Y, N}`
* Monitored = `{Y, N}`
* Scope = `{EGI, Local}`

##### Data feeds

Source of the data for other connectors like `weights-vapor-connector.py` and `downtimes-gocdb-connector.py` are optional and can be specified per job. If specified, they will override their default source of data. Example:

	[JOB_BioMedCritical]
	Dirname = BioMed_Critical
	DowntimesFeed = https://goc.egi.eu/gocdbpi/
	Profiles = ROC_CRITICAL
	TopoSelectGroupOfGroups = Monitored:Y, Scope:EGI
	WeightsFeed = http://gstat2.grid.sinica.edu.tw/gstat/summary/json/ 

`WeightsFeed` and `DowntimesFeed` are alternative data feeds for this job for connectors `weights-vapor-connector.py` and `downtimes-gocdb-connector.py`, respectively.

<a id="sync3"></a>

### poem-connector.conf

This configuration file is central configuration for `poem-connector.py` whose role is:

- fetch all defined POEM profiles from each POEM server specified
- prepare and layout data needed for `prefilter-egi.py`

Default location of `poem-connector.conf` can be overriden with `-p` argument of connector:

	poem-connector.py -p /path/to/poem-connector.conf

#### POEM profiles fetch

Config file is splitted into a few sections:

	[PoemServer]
	Host = snf-624922.vm.okeanos.grnet.gr
	VO = ops

This section defines the URL where POEM server is located and all VOes for which POEM profiles will be fetched. Multiple POEM servers can be specified by defining multiple POEM server sections:

	[PoemServer1]
	Host = poem1
	VO = vo1, vo2

	[PoemServer2]
	Host = poem2
	VO = vo3, vo4

Same POEM profile can be defined on multiple POEM servers. Each POEM server can further extend it with a custom combinations of metrics and service flavours. To distinguish POEM profile defined on multiple POEM servers, namespace must be used. One must be aware of the namespace that POEM server exposes and specify it in `FetchProfiles` section:

	[FetchProfiles]
	List = CH.CERN.SAM.ROC, CH.CERN.SAM.ROC_OPERATORS, CH.CERN.SAM.ROC_CRITICAL, CH.CERN.SAM.OPS_MONITOR, CH.CERN.SAM.OPS_MONITOR_CRITICAL, CH.CERN.SAM.GLEXEC, CH.CERN.SAM.CLOUD-MON

#### Prefilter data and `prefilter-egi.py` component

`poem-connector.py` also generates plaintext `PoemExpandedProfiles` file (specified in `global.conf`) on a daily basis for EGI customer and places it under its directory. It's not generated for every EGI subcustomer that may be specified in `customer.conf` but only for main one - EGI. Also, it can be invoked to **not** generate `PoemExpandedProfiles` file at all for customers that don't need it (i.e., customers outside EGI):

	poem-connector.py -np

Content of the `PoemExpandedProfiles` file is controlled in `[PrefilterData]` section:

	[PrefilterData]
	AllowedNGI = http://mon.egi.eu/nagios-roles.conf
	AllowedNGIProfiles = ch.cern.sam.ROC, ch.cern.sam.ROC_OPERATORS, ch.cern.sam.ROC_CRITICAL, ch.cern.sam.GLEXEC
	AllNGI1 = opsmon.egi.eu
	AllNGIProfiles1 = ch.cern.sam.OPS_MONITOR, ch.cern.sam.OPS_MONITOR_CRITICAL
	AllNGI2 = cloudmon.egi.eu
	AllNGIProfiles2 = ch.cern.sam.CLOUD-MON

`AllowedNGI` option defines remote config file that states all allowed NGIes and corresponding nagios boxes. All of them will be expanded and listed together with the information from `AllowedNGIProfiles` POEM profiles (metrics, service flavours, VOes). 

`AllNGI1` option is similar in sense that it will extended specified nagios box (monitoring instance) with the information from `AllNGIProfiles1` POEM profiles. Multiple `AllNGI*` options can be specified and they must come in pair fashion so for every `AllNGI[n]` option, there must exist `AllNGIProfiles[n]` option that is related to it.

With all these informations written in `PoemExpandedProfiles` file, `prefilter-egi.py` can do its work, so it will filter consumer messages if:

- message that enter into broker network doesn't come from allowed NGI or nagios box for certain NGI is incorrectly specified
- metric result is response to metric not found in a fetched service flavour
- metric result's service flavour is not registered in any fetched POEM profile
- metric result is registered for different VO, not specified in `VO` option of any `[PoemServer]` section

All `prefilter-egi.py` related I/O options are in `global.conf`:

	[DEFAULT]
	EGIDir = /var/lib/argo-connectors/EGI/

* input

	[Prefilter]
	ConsumerFilePath = /var/lib/argo-egi-consumer/argo-consumer_log_DATE.avro
	PoemExpandedProfiles = %(EGIDir)s/poem_sync_DATE.out`

* output

	[Output]
	Prefilter = %(EGIDir)s/prefilter_DATE.avro

If `prefilter-egi.py` is intended to be used for multiple customers, one must define `global.conf` for each customer and pass it to `prefilter-egi.py` while it's being called:

	prefilter-egi.py -g /path/to/global-cus1.conf
	prefilter-egi.py -g /path/to/global-cus2.conf


## Examples

<div role="tabpanel">

  <!-- Nav tabs -->
  <ul class="nav nav-tabs" role="tablist">
    <li role="presentation" class="active"><a href="#customer.conf" aria-controls="customer.conf" role="tab" data-toggle="tab">customer.conf</a></li>
    <li role="presentation"><a href="#customer-jobs" aria-controls="customer-jobs" role="tab" data-toggle="tab">Customer jobs</a></li>
    <li role="presentation"><a href="#prefilterdata" aria-controls="prefilterdata" role="tab" data-toggle="tab">Prefilter data</a></li>
    <li role="presentation"><a href="#JOB_Critical" aria-controls="JOB_Critical" role="tab" data-toggle="tab">EGI JOB_Critical</a></li>
    <li role="presentation"><a href="#JOB_Cloudmon" aria-controls="JOB_Cloudmon" role="tab" data-toggle="tab">EGI JOB_Cloudmon</a></li>
    <li role="presentation"><a href="#JOB_BioMedCritical" aria-controls="JOB_BioMedCritical" role="tab" data-toggle="tab">JOB_BioMedCritical</a></li>
    <li role="presentation"><a href="#JOB_BioMedCloudmon" aria-controls="JOB_BioMedCloudmon" role="tab" data-toggle="tab">VO JOB_BioMedCloudmon</a></li>
    <li role="presentation"><a href="#downtimes" aria-controls="downtimes" role="tab" data-toggle="tab">Downtimes</a></li>
  </ul>

  <!-- Tab panes -->
  <div class="tab-content">
    <div role="tabpanel" class="tab-pane active" id="customer.conf">
<p>&nbsp;</p>
<strong>customer.conf:</strong>
<p>&nbsp;</p>
<pre>
	[DIR]
	OutputDir = /var/lib/argo-connectors/EGI/

	[CUSTOMER_EGI]
	Jobs = JOB_EGICritical, JOB_EGICloudmon, JOB_BioMedCloudmon, JOB_BioMedCritical

	[JOB_EGICritical]
	Dirname = EGI_Critical
	Profiles = ROC_CRITICAL
	TopoFetchType = Sites
	#TopoSelectGroupOfEndpoints = Production:Y, Monitored:Y, Scope:EGI
	TopoSelectGroupOfGroups = Certification:Uncertified, Infrastructure:Test, Scope:EGI

	[JOB_EGICloudmon]
	Dirname = EGI_Cloudmon
	Profiles = CLOUD-MON
	TopoFetchType = ServiceGroups
	TopoSelectGroupOfEndpoints = Monitored:Y, Scope:EGI, Production:N
	#TopoSelectGroupOfGroups = Monitored:Y, Scope:EGI, Certification:(Certified,Candidate)
</pre>

    </div>
    <div role="tabpanel" class="tab-pane" id="customer-jobs">

<p>&nbsp;</p>
<strong>Customer jobs:</strong>
<p>&nbsp;</p>
<pre>
	/var/lib/argo-connectors/EGI/EGI_Cloudmon/group_endpoints_2015_04_07.avro
	/var/lib/argo-connectors/EGI/EGI_Cloudmon/group_groups_2015_04_07.avro
	/var/lib/argo-connectors/EGI/EGI_Cloudmon/poem_sync_2015_04_07.avro
	/var/lib/argo-connectors/EGI/EGI_Cloudmon/weights_2015_04_07.avro
	/var/lib/argo-connectors/EGI/EGI_Critical/group_endpoints_2015_04_07.avro
	/var/lib/argo-connectors/EGI/EGI_Critical/group_groups_2015_04_07.avro
	/var/lib/argo-connectors/EGI/EGI_Critical/poem_sync_2015_04_07.avro
	/var/lib/argo-connectors/EGI/EGI_Critical/weights_2015_04_07.avro
</pre>

    </div>
    <div role="tabpanel" class="tab-pane" id="prefilterdata">

<p>&nbsp;</p>

<strong>Prefilter data:</strong>

<p>&nbsp;</p>
<pre>

	/var/lib/argo-connectors/EGI/poem_sync_2015_04_07.out
	/var/lib/argo-connectors/EGI/prefilter_2015_04_07.avro

</pre>
</div>
 <div role="tabpanel" class="tab-pane" id="JOB_Critical">

<p>&nbsp;</p>
For customer's job JOB_EGICritical, we are selecting only those sites that match `Certification:Uncertified`,  `Infrastructure:Test` and `Scope:EGI`, so in `TopologyGroupOfGroups` file there will be only those sites listed:
<p>&nbsp;</p>
<pre>
	 % avro cat /var/lib/argo-connectors/EGI/EGI_Critical/group_groups_2015_04_07.avro | tail -n 1
	 {"group": "Russia", "tags": {"scope": "EGI", "infrastructure": "Test", "certification": "Uncertified"}, "type": "NGI", "subgroup": "SU-Protvino-IHEP"}
</pre>
<p>&nbsp;</p>
    </div>
    <div role="tabpanel" class="tab-pane" id="JOB_Cloudmon">
<p>&nbsp;</p>
 For customer's JOB_EGICloudmon, we are selecting only those service endpoints that match `Monitored:Y`, `Scope:EGI`, `Production:N`:

<p>&nbsp;</p>

<pre>	 % avro cat /var/lib/argo-connectors/EGI/EGI_Cloudmon/group_endpoints_2015_04_07.avro
	 {"group": "ROC_RU_SERVICE", "hostname": "ce.ngc6475.ihep.su", "type": "SERVICEGROUPS", "service": "Top-BDII", "tags": {"scope": "EGI", "production": 0, "monitored": 1}}
</pre>

<p>&nbsp;</p>
</div>

<div role="tabpanel" class="tab-pane" id="downtimes">
<p>&nbsp;</p>
<strong>Downtimes:</strong>
<p>&nbsp;</p>
<pre>
	% /usr/libexec/argo-egi-connectors/downtimes-gocdb-connector.py -d 2015-04-07
	% find /var/lib/argo-connectors -name '*downtimes*'
	/var/lib/argo-connectors/EGI/EGI_Cloudmon/downtimes_2015_04_07.avro
	/var/lib/argo-connectors/EGI/EGI_Critical/downtimes_2015_04_07.avro
</pre>
</div>
  </div>

</div>

## Links

Connectors are using following GOCDB PI methods:

- [GOCDB - get_downtime_method](https://wiki.egi.eu/wiki/GOCDB/PI/get_downtime_method)
- [GOCDB - get_service_endpoint_method](https://wiki.egi.eu/wiki/GOCDB/PI/get_service_endpoint_method)
- [GOCDB - get_service_group](https://wiki.egi.eu/wiki/GOCDB/PI/get_service_group)
- [GOCDB - get_site_method](https://wiki.egi.eu/wiki/GOCDB/PI/get_site_method)

`poem-connector.py` is using POEM PI method:
- [POEM - metrics_in_profiles](http://argoeu.github.io/guides/poem/)

import ConfigParser
import os, re, errno
from log import Logger

class Global:
    """
       Class represents parser for global.conf
    """
    # options common for all connectors
    conf_ams = {'AMS': ['Host', 'Token', 'Project', 'Topic', 'Bulk']}
    conf_general = {'General': ['PublishAms', 'WriteAvro']}
    conf_certs = {'Authentication': ['HostKey', 'HostCert', 'CAPath', 'CAFile',
                                     'VerifyServerCert']}
    conf_conn = {'Connection': ['Timeout', 'Retry']}
    conf_state = {'InputState': ['SaveDir', 'Days']}

    # options specific for every connector
    conf_topo_schemas = {'AvroSchemas': ['TopologyGroupOfEndpoints',
                                         'TopologyGroupOfGroups']}
    conf_topo_output = {'Output': ['TopologyGroupOfEndpoints',
                                   'TopologyGroupOfGroups']}
    conf_downtimes_schemas = {'AvroSchemas': ['Downtimes']}
    conf_downtimes_output = {'Output': ['Downtimes']}
    conf_weights_schemas = {'AvroSchemas': ['Weights']}
    conf_weights_output = {'Output': ['Weights']}
    conf_poem_output = {'Output': ['Poem']}
    conf_poem_schemas = {'AvroSchemas': ['Poem']}
    conf_poem_prefilter = {'Prefilter': ['PoemExpandedProfiles']}
    conf_prefilter_prefilter = {'Prefilter': ['ConsumerFilePath',
                                              'PoemExpandedProfiles',
                                              'PoemNameMapping',
                                              'LookbackPoemExpandedProfiles']}
    conf_prefilter_schemas = {'AvroSchemas': ['Prefilter']}
    conf_prefilter_output = {'Output': ['Prefilter']}

    def __init__(self, caller, confpath, **kwargs):
        self.optional = dict()

        self.logger = Logger(str(self.__class__))
        self._filename = '/etc/argo-egi-connectors/global.conf' if not confpath else confpath
        self._checkpath = kwargs['checkpath'] if 'checkpath' in kwargs.keys() else False

        self.optional.update(self._lowercase_dict(self.conf_ams))

        self.shared_secopts = self._merge_dict(self.conf_ams,
                                               self.conf_general,
                                               self.conf_certs, self.conf_conn,
                                               self.conf_state)
        self.secopts = {'topology-gocdb-connector.py':
                        self._merge_dict(self.shared_secopts,
                                         self.conf_topo_schemas,
                                         self.conf_topo_output),
                        'downtimes-gocdb-connector.py':
                        self._merge_dict(self.shared_secopts,
                                         self.conf_downtimes_schemas,
                                         self.conf_downtimes_output),
                        'weights-vapor-connector.py':
                        self._merge_dict(self.shared_secopts,
                                         self.conf_weights_schemas,
                                         self.conf_weights_output),
                        'poem-connector.py':
                        self._merge_dict(self.shared_secopts,
                                         self.conf_poem_schemas,
                                         self.conf_poem_output,
                                         self.conf_poem_prefilter),
                        'prefilter-egi.py':
                        self._merge_dict(self.conf_prefilter_output,
                                         self.conf_prefilter_schemas,
                                         self.conf_prefilter_prefilter)
                        }

        self.caller_secopts = self.secopts[os.path.basename(caller)]

    def _merge_dict(self, *args):
        newd = dict()
        for d in args:
            newd.update(d)
        return newd

    def _lowercase_dict(self, d):
        newd = dict()
        for k in d.iterkeys():
            opts = [o.lower() for o in d[k]]
            newd[k.lower()] = opts
        return newd

    def merge_opts(self, custamsopt, section):
        newd = custamsopt.copy()
        opts = [o for o in self.options.keys() if o.startswith(section)]
        for o in opts:
            if o in newd:
                continue
            newd.update({o: self.options[o]})

        return newd

    def is_complete(self, opts, section):
        all = set([section+o for o in self.optional[section]])
        diff = all.symmetric_difference(opts.keys())
        if diff:
            return (False, diff)
        return (True, None)

    def parse(self):
        config = ConfigParser.ConfigParser()

        if not os.path.exists(self._filename):
            self.logger.error('Could not find %s' % self._filename)
            raise SystemExit(1)

        config.read(self._filename)
        options = {}

        lower_section = [sec.lower() for sec in config.sections()]

        try:
            for sect, opts in self.caller_secopts.items():
                if (sect.lower() not in lower_section and
                    sect.lower() not in self.optional.keys()):
                    raise ConfigParser.NoSectionError(sect.lower())

                for opt in opts:
                    for section in config.sections():
                        if section.lower().startswith(sect.lower()):
                            try:
                                optget = config.get(section, opt)
                                if self._checkpath and os.path.isfile(optget) is False:
                                    raise OSError(errno.ENOENT, optget)

                                if ('output' in section.lower() and
                                    'DATE' not in optget):
                                        logger.error('No DATE placeholder in %s' % option)
                                        raise SystemExit(1)

                                options.update({(sect+opt).lower(): optget})

                            except ConfigParser.NoOptionError as e:
                                s = e.section.lower()
                                if (s in self.optional.keys() and
                                    e.option in self.optional[s]):
                                    pass
                                else:
                                    raise e

        except ConfigParser.NoOptionError as e:
            self.logger.error(e.message)
            raise SystemExit(1)
        except ConfigParser.NoSectionError as e:
            self.logger.error("%s defined" % (e.args[0]))
            raise SystemExit(1)
        except OSError as e:
            self.logger.error('%s %s' % (os.strerror(e.args[0]), e.args[1]))
            raise SystemExit(1)

        self.options = options

        return options

class PoemConf:
    options = {}

    def __init__(self, confpath, *args):
        self.logger = Logger(str(self.__class__))
        self._args = args
        self._filename = '/etc/argo-egi-connectors/poem-connector.conf' if not confpath else confpath

    def parse(self):
        config = ConfigParser.ConfigParser()
        if not os.path.exists(self._filename):
            self.logger.error('Could not find %s' % self._filename)
            raise SystemExit(1)
        config.read(self._filename)

        try:
            for arg in self._args:
                for sect, opts in arg.items():
                    for opt in opts:
                        for section in config.sections():
                            if section.lower().startswith(sect.lower()):
                                lopts = config.options(section)
                                for o in lopts:
                                    if o.startswith(opt.lower()):
                                        optget = config.get(section, o)
                                        self.options.update({(section+o).lower(): optget})

        except ConfigParser.NoOptionError as e:
            self.logger.error(e.message)
            raise SystemExit(1)
        except ConfigParser.NoSectionError as e:
            self.logger.error("No section '%s' defined" % (e.args[0]))
            raise SystemExit(1)

        return self.options

    def _get_ngis(self, option):
        ngis = {}

        def filtkey(elem):
            if option in elem and not\
                    re.search('profiles[0-9]*', elem):
                return True
        for opt in filter(filtkey, self.options.keys()):
            match = re.search('(%s)([0-9]+$)' % option, opt)
            if match:
                value = match.group(1)+'profiles'+match.group(2)
                ngis.update({self.options[opt]:
                                re.split('\s*,\s*', self.options[value])})
            elif option == opt:
                ngis.update({self.options[opt]:
                                re.split('\s*,\s*', self.options[opt+'profiles'])})

        return ngis

    def get_allngi(self):
        try:
            return self._get_ngis('PrefilterDataAllNGI'.lower())
        except KeyError as e:
            self.logger.error("No option %s defined" % e)
            raise SystemExit(1)

    def get_servers(self):
        poemservers = {}
        for opt in self.options.keys():
            if 'PoemServer'.lower() in opt:
                key = re.search('\w+[0-9]+', opt)
                key = 'PoemServer'.lower() if not key else key.group(0)
                poemservers.update({self.options[key+'host']:
                                    re.split('\s*,\s*', self.options[key+'vo'])})
        return poemservers

class CustomerConf:
    """
       Class with parser for customer.conf and additional helper methods
    """
    _custattrs = None
    _cust = {}
    _defjobattrs = {'topology-gocdb-connector.py' : ['TopoFetchType',
                                                     'TopoSelectGroupOfGroups',
                                                     'TopoSelectGroupOfEndpoints',
                                                     'TopoType','TopoFeed'],
                    'topology-vo-connector.py': ['TopoSelectGroupOfGroups',
                                                 'TopoType', 'TopoFeed'],
                    'poem-connector.py': [],
                    'downtimes-gocdb-connector.py': ['DowntimesFeed'],
                    'weights-vapor-connector.py': ['WeightsFeed'],
                    'prefilter-egi.py': []}
    _jobs, _jobattrs = {}, None
    _cust_optional = ['AmsHost', 'AmsProject', 'AmsToken', 'AmsTopic']
    tenantdir = ''

    def __init__(self, caller, confpath, **kwargs):
        self.logger = Logger(str(self.__class__))
        self._filename = '/etc/argo-egi-connectors/customer.conf' if not confpath else confpath
        if not kwargs:
            self._jobattrs = self._defjobattrs[os.path.basename(caller)]
        else:
            if 'jobattrs' in kwargs.keys():
                self._jobattrs = kwargs['jobattrs']
            if 'custattrs' in kwargs.keys():
                self._custattrs = kwargs['custattrs']

    def parse(self):
        config = ConfigParser.ConfigParser()
        if not os.path.exists(self._filename):
            self.logger.error('Could not find %s' % self._filename)
            raise SystemExit(1)
        config.read(self._filename)

        lower_custopt = [oo.lower() for oo in self._cust_optional]

        for section in config.sections():
            if section.lower().startswith('CUSTOMER_'.lower()):
                amsopts = dict()
                amshost, amstoken, amsproject, amstopic = None, None, None, None

                try:
                    custjobs = config.get(section, 'Jobs').split(',')
                    custjobs = [job.strip() for job in custjobs]
                    custdir = config.get(section, 'OutputDir')
                    custname = config.get(section, 'Name')

                    for o in lower_custopt:
                        try:
                            code = "amsopts.update(%s = config.get(section, '%s'))" % (o, o)
                            exec code
                        except ConfigParser.NoOptionError as e:
                            if e.option in lower_custopt:
                                pass
                            else:
                                raise e

                except ConfigParser.NoOptionError as e:
                    self.logger.error(e.message)
                    raise SystemExit(1)

                self._cust.update({section: {'Jobs': custjobs, 'OutputDir': custdir, 'Name': custname}})
                if amsopts:
                    self._cust[section].update(AmsOpts=amsopts)

                if self._custattrs:
                    for attr in self._custattrs:
                        if config.has_option(section, attr):
                            self._cust[section].update({attr: config.get(section, attr)})

        for cust in self._cust:
            for job in self._cust[cust]['Jobs']:
                if config.has_section(job):
                    try:
                        profiles = config.get(job, 'Profiles')
                        dirname = config.get(job, 'Dirname')
                    except ConfigParser.NoOptionError as e:
                        self.logger.error(e.message)
                        raise SystemExit(1)

                    self._jobs.update({job: {'Profiles': profiles, 'Dirname': dirname}})
                    if self._jobattrs:
                        for attr in self._jobattrs:
                            if config.has_option(job, attr):
                                self._jobs[job].update({attr: config.get(job, attr)})
                else:
                    self.logger.error("Could not find Jobs: %s for customer: %s" % (job, cust))
                    raise SystemExit(1)

    def _sect_to_dir(self, sect):
        try:
            match = re.match('(?:^\w+?_)(\w+)', sect)
            assert match != None
            dirname = match.group(1)
        except (AssertionError, KeyError) as e:
            self.logger.error("Could not get Dirname for %s" % e)
            raise SystemExit(1)
        return dirname

    def _dir_from_sect(self, sect, d):
        dirname = ''

        for k, v in d.items():
            if k == sect:
                if 'Dirname' in v.keys():
                    dirname = v['Dirname']
                elif 'OutputDir' in v.keys():
                    dirname = v['OutputDir']
                else:
                    dirname = self._sect_to_dir(sect)

        return dirname

    def get_jobdir(self, job):
        return self._dir_from_sect(job, self._jobs)

    def get_amsopts(self, cust):
        if 'AmsOpts' in self._cust[cust]:
            return self._cust[cust]['AmsOpts']
        else:
            return dict()

    def get_fulldir(self, cust, job):
        return self.get_custdir(cust) + '/' + self.get_jobdir(job) + '/'

    def get_fullstatedir(self, root, cust, job):
        return root + '/' + self.get_custname(cust) + '/' + self.get_jobdir(job)

    def get_custdir(self, cust):
        return self._dir_from_sect(cust, self._cust)

    def get_custname(self, cust):
        return self._cust[cust]['Name']

    def make_dirstruct(self, root=None):
        dirs = []
        for cust in self._cust.keys():
            for job in self.get_jobs(cust):
                if root:
                    dirs.append(root + '/' + self.get_custname(cust) + '/' + self.get_jobdir(job))
                else:
                    dirs.append(self.get_custdir(cust) + '/' + self.get_jobdir(job))
            for d in dirs:
                try:
                    os.makedirs(d)
                except OSError as e:
                    if e.args[0] != errno.EEXIST:
                        self.logger.error('%s %s %s' % (os.strerror(e.args[0]), e.args[1], d))
                        raise SystemExit(1)

    def get_jobs(self, cust):
        jobs = []
        try:
            jobs = self._cust[cust]['Jobs']
        except KeyError:
            self.logger.error("Could not get Jobs for %s" % cust)
            raise SystemExit(1)
        return jobs

    def get_customers(self):
        return self._cust.keys()

    def get_profiles(self, job):
        profiles = self._jobs[job]['Profiles'].split(',')
        for i, p in enumerate(profiles):
            profiles[i] = p.strip()
        return profiles

    def get_gocdb_fetchtype(self, job):
        return self._jobs[job]['TopoFetchType']

    def _get_tags(self, job, option):
        tags = {}
        if option in self._jobs[job].keys():
            tagstr = self._jobs[job][option]
            match = re.findall("(\w+)\s*:\s*(\(.*?\))", tagstr)
            if match is not None:
                for m in match:
                    tags.update({m[0]: [e.strip('() ') for e in m[1].split(',')]})
            match = re.findall('([\w]+)\s*:\s*([\w\.\-\_]+)', tagstr)
            if match is not None:
                for m in match:
                    tags.update({m[0]: m[1]})
            else:
                self.logger.error("Could not parse option %s: %s" % (option, tagstr))
                return dict()
        return tags

    def get_gocdb_ggtags(self, job):
        return self._get_tags(job, 'TopoSelectGroupOfGroups')

    def get_gocdb_getags(self, job):
        return self._get_tags(job, 'TopoSelectGroupOfEndpoints')

    def get_vo_ggtags(self, job):
        return self._get_tags(job, 'TopoSelectGroupOfGroups')

    def _get_toponame(self, job):
        return self._jobs[job]['TopoType']

    def _get_feed(self, job, key):
        try:
            feed = self._jobs[job][key]
        except KeyError:
            feed = ''
        return feed

    def _update_feeds(self, feeds, feedurl, job, cust):
        if feedurl in feeds.keys():
            feeds[feedurl].append((job, cust))
        elif feedurl:
            feeds[feedurl] = []
            feeds[feedurl].append((job, cust))

    def get_feedscopes(self, feed, jobcust):
        ggtags, getags = [], []
        distinct_scopes = set()
        for job, cust in jobcust:
            gg = self._get_tags(job, 'TopoSelectGroupOfGroups')
            ge = self._get_tags(job, 'TopoSelectGroupOfEndpoints')
            for g in gg.items() + ge.items():
                if 'Scope'.lower() == g[0].lower():
                    if isinstance(g[1], list):
                        distinct_scopes.update(g[1])
                    else:
                        distinct_scopes.update([g[1]])

        return distinct_scopes

    def get_mapfeedjobs(self, caller, name=None, deffeed=None):
        feeds = {}
        for c in self.get_customers():
            for job in self.get_jobs(c):
                if 'topology' in caller:
                    if self._get_toponame(job) == name:
                        feedurl = self._get_feed(job, 'TopoFeed')
                        if feedurl:
                            self._update_feeds(feeds, feedurl, job, c)
                        elif not feedurl and name == 'VOFeed':
                            self.logger.error("Could not get VO TopoFeed for job %s" % job)
                            raise SystemExit(1)
                        else:
                            feedurl = deffeed
                            self._update_feeds(feeds, feedurl, job, c)
                elif 'downtimes' in caller:
                    feedurl = self._get_feed(job, 'DowntimesFeed')
                    if feedurl:
                        self._update_feeds(feeds, feedurl, job, c)
                    else:
                        feedurl = deffeed
                        self._update_feeds(feeds, feedurl, job, c)
                elif 'weights' in caller:
                    feedurl = self._get_feed(job, 'WeightsFeed')
                    if feedurl:
                        self._update_feeds(feeds, feedurl, job, c)
                    else:
                        feedurl = deffeed
                        self._update_feeds(feeds, feedurl, job, c)

        return feeds

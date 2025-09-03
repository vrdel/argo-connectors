import configparser
import errno
import os

from argo_connectors.log import Logger


class Global(object):
    """
       Class represents parser for global.conf
    """
    # options common for all connectors
    conf_general = {'General': ['WriteJson',
                                'PublishWebAPI', 'PassExtensions', 'CompressJson']}
    conf_auth = {'Authentication': ['HostKey', 'HostCert', 'CAPath', 'CAFile',
                                    'VerifyServerCert', 'UsePlainHttpAuth',
                                    'HttpUser', 'HttpPass']}
    conf_conn = {'Connection': ['Timeout', 'Retry', 'SleepRetry', 'RetryRandom', 'SleepRandomRetryMax']}
    conf_state = {'InputState': ['SaveDir', 'Days']}
    conf_webapi = {'WebAPI': ['Token', 'Host']}

    # options specific for every connector
    conf_topo_output = {'Output': ['TopologyGroupOfEndpoints',
                                   'TopologyGroupOfGroups']}
    conf_downtimes_output = {'Output': ['Downtimes']}
    conf_weights_output = {'Output': ['Weights']}
    conf_metricprofile_output = {'Output': ['MetricProfile']}

    def __init__(self, caller, confpath=None, **kwargs):
        self.optional = dict()

        self.logger = Logger(str(self.__class__))
        self._filename = f"{os.environ['VIRTUAL_ENV']}/etc/global.conf" if not confpath else confpath

        self._checkpath = kwargs['checkpath'] if 'checkpath' in kwargs.keys(
        ) else False

        self.optional.update(self._lowercase_dict(self.conf_auth))
        self.optional.update(self._lowercase_dict(self.conf_webapi))

        self.shared_secopts = self._merge_dict(self.conf_general,
                                               self.conf_auth, self.conf_conn,
                                               self.conf_state,
                                               self.conf_webapi)
        self.secopts = {
            'topology-gocdb-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'topology-json-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'topology-provider-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'topology-lot1sc-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'topology-agora-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'downtimes-csv-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_downtimes_output),
            'downtimes-gocdb-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_downtimes_output),
            'weights-vapor-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_weights_output),
            'topology-csv-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'metricprofile-webapi-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_metricprofile_output),
            'service-types-gocdb-connector.py':
            self._merge_dict(self.shared_secopts),
            'service-types-csv-connector.py':
            self._merge_dict(self.shared_secopts),
            'service-types-json-connector.py':
            self._merge_dict(self.shared_secopts),

        }

        if caller:
            self.caller_secopts = self.secopts[os.path.basename(caller)]
        else:
            self.caller_secopts = self.shared_secopts

    def _merge_dict(self, *args):
        newd = dict()
        for d in args:
            newd.update(d)
        return newd

    def _lowercase_dict(self, d):
        newd = dict()
        for k in d.keys():
            opts = [o.lower() for o in d[k]]
            newd[k.lower()] = opts
        return newd

    def merge_opts(self, custopt, section):
        newd = custopt.copy()
        opts = [o for o in self.options.keys() if o.startswith(section)]
        for o in opts:
            if o in newd:
                continue
            newd.update({o: self.options[o]})

        return newd

    def is_complete(self, opts, section):
        all = set([section + o for o in self.optional[section]])
        diff = all.symmetric_difference(opts.keys())
        if diff:
            return (False, diff)
        return (True, None)

    def _concat_sectopt(self, d):
        opts = list()

        for k in d.keys():
            for v in d[k]:
                opts.append(k + v)

        return opts

    def _one_active(self, options):
        loweropts = self._lowercase_dict(options)

        lval = [eval(self.options[k]) for k in self._concat_sectopt(loweropts)]

        if any(lval):
            return True
        else:
            return False

    def parse(self):
        config = configparser.ConfigParser()

        if not os.path.exists(self._filename):
            self.logger.error('Could not find %s' % self._filename)
            raise SystemExit(1)

        config.read(self._filename)
        options = {}

        lower_section = [sec.lower() for sec in config.sections()]

        try:
            for sect, opts in self.caller_secopts.items():
                if (sect.lower() not in lower_section and sect.lower() not in
                        self.optional.keys()):
                    raise configparser.NoSectionError(sect.lower())

                for opt in opts:
                    for section in config.sections():
                        if section.lower().startswith(sect.lower()):
                            try:
                                optget = config.get(section, opt)
                                if self._checkpath and os.path.isfile(optget) is False:
                                    raise OSError(errno.ENOENT, optget)

                                if ('output' in section.lower() and 'DATE' not
                                        in optget):
                                    self.logger.error(
                                        'No DATE placeholder in %s' % opt)
                                    raise SystemExit(1)

                                options.update({(sect + opt).lower(): optget})

                            except configparser.NoOptionError as e:
                                s = e.section.lower()
                                if (s in self.optional.keys() and
                                        e.option in self.optional[s]):
                                    pass
                                else:
                                    raise e

            self.options = options

            if not self._one_active(self.conf_general):
                self.logger.error('At least one of %s needs to be True' % (
                    ', '.join(self._concat_sectopt(self.conf_general))))
                raise SystemExit(1)

        except configparser.NoOptionError as e:
            self.logger.error(e.message)
            raise SystemExit(1)
        except configparser.NoSectionError as e:
            self.logger.error("%s defined" % (e.args[0]))
            raise SystemExit(1)
        except OSError as e:
            self.logger.error('%s %s' % (os.strerror(e.args[0]), e.args[1]))
            raise SystemExit(1)

        return options

from distutils.core import setup
import glob

NAME = 'argo-egi-connectors'


def get_ver():
    try:
        for line in open(NAME + '.spec'):
            if "Version:" in line:
                return line.split()[1]
    except IOError:
        print("Make sure that {} is in directory".format(NAME + '.spec'))
        raise SystemExit(1)


setup(name=NAME,
      version=get_ver(),
      author='SRCE',
      author_email='dvrcic@srce.hr, kzailac@srce.hr',
      description='Components generate input data for ARGO Compute Engine',
      url='http://argoeu.github.io/guides/sync/',
      package_dir={'argo_egi_connectors': 'modules/'},
      packages=['argo_egi_connectors'],
      data_files=[('/etc/argo-egi-connectors', glob.glob('etc/*.conf.template')),
                  ('/usr/libexec/argo-egi-connectors', ['bin/downtimes-gocdb-connector.py',
                                                        'bin/metricprofile-webapi-connector.py',
                                                        'bin/topology-gocdb-connector.py',
                                                        'bin/topology-eosc-connector.py',
                                                        'bin/weights-vapor-connector.py',
                                                        'bin/replay-avro-data.py']),
                  ('/etc/argo-egi-connectors/schemas', glob.glob('etc/schemas/*.avsc'))])

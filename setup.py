from distutils.core import setup
import glob


NAME = 'argo-connectors'


setup(name=NAME,
      version="2.3.0",
      author='SRCE',
      author_email='dvrcic@srce.hr',
      description='Components generate input data for ARGO Compute Engine',
      classifiers=[
          "Development Status :: 5 - Production/Stable",
          "License :: OSI Approved :: Apache Software License",
          "Operating System :: POSIX",
          "Programming Language :: Python :: 3.9",
          "Intended Audience :: Developers",
      ],
      url='http://argoeu.github.io/guides/sync/',
      package_dir={'argo_connectors.io': 'modules/io',
                   'argo_connectors.parse': 'modules/parse',
                   'argo_connectors.mesh': 'modules/mesh',
                   'argo_connectors.tasks': 'modules/tasks',
                   'argo_connectors': 'modules/'},
      packages=['argo_connectors', 'argo_connectors.io',
                'argo_connectors.parse', 'argo_connectors.mesh',
                'argo_connectors.tasks'],
      data_files=[('/etc/argo-connectors', glob.glob('etc/*.conf.template')),
                  ('/usr/libexec/argo-connectors', [
                      'exec/downtimes-csv-connector.py',
                      'exec/downtimes-gocdb-connector.py',
                      'exec/metricprofile-webapi-connector.py',
                      'exec/service-types-csv-connector.py',
                      'exec/service-types-gocdb-connector.py',
                      'exec/service-types-json-connector.py',
                      'exec/topology-csv-connector.py',
                      'exec/topology-gocdb-connector.py',
                      'exec/topology-json-connector.py',
                      'exec/topology-provider-connector.py',
                      'exec/topology-agora-connector.py',
                      'exec/weights-vapor-connector.py',
                  ])
                ])

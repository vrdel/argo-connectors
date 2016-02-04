#!/usr/bin/python

# Copyright (c) 2013 GRNET S.A., SRCE, IN2P3 CNRS Computing Centre
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS
# IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language
# governing permissions and limitations under the License.
#
# The views and conclusions contained in the software and
# documentation are those of the authors and should not be
# interpreted as representing official policies, either expressed
# or implied, of either GRNET S.A., SRCE or IN2P3 CNRS Computing
# Centre
#
# The work represented by this source file is partially funded by
# the EGI-InSPIRE project through the European Commission's 7th
# Framework Programme (contract # INFSO-RI-261323)

import optparse
import avro.schema
import datetime
import os
import sys
import time

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.writers import Logger
from argo_egi_connectors.tools import gen_fname_repdate
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

globopts, confcust = {}, None
logger = None

#poem
poemFileFields = 'server;ngi;profile;service_flavour;metric;vo;fqan'
poemFileFieldDelimiter = '\001'

# reject on missing monitoring host
rejectMissingMonitoringHost = 0

def poemProfileFilenameCheck(year, month, day):
    count = 0
    dt = datetime.datetime(year=int(year), month=int(month), day=int(day))
    while True:
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        fileName = gen_fname_repdate(logger, year+'_'+month+'_'+day, globopts['PrefilterPoemExpandedProfiles'.lower()], '')
        if os.path.isfile(fileName):
            break
        if count >= int(globopts['PrefilterLookbackPoemExpandedProfiles'.lower()]):
            fileName = None
            break
        count = count+1
        dt = dt - datetime.timedelta(days=1)

    return fileName

def loadNGIs(year, month, day):
    ngiTree = dict()

    profileFieldNames = poemFileFields.split(';')

    try:
        poemfile = poemProfileFilenameCheck(year, month, day)
        assert poemfile is not None
        poemProfileFile = open(poemfile, 'r')
    except (IOError, AssertionError) as e:
        logger.error('Cannot open POEM file with expanded profiles for every monitoring instance')
        raise SystemExit(1)

    poemProfiles = poemProfileFile.read().splitlines()
    poemProfileFile.close()

    for line in poemProfiles:
        if len(line) == 0 or line[0] == '#':
            continue

        profileFields = line.split(poemFileFieldDelimiter)
        profile = dict()
        for i in range(0, len(profileFieldNames)):
            profile[profileFieldNames[i]] = profileFields[i]

        if profile['server'] not in ngiTree:
            ngiTree[profile['server']] = list()

        if profile['ngi'] not in ngiTree[profile['server']]:
            ngiTree[profile['server']].append(profile['ngi'])

        # ngiTree[profile['server']] = profile['ngi']

    return ngiTree

def loadFilteredProfiles(year, month, day):
    profileTree = dict()

    profileFieldNames = poemFileFields.split(';')

    poemProfileFile = open(poemProfileFilenameCheck(year, month, day), 'r')
    poemProfiles = poemProfileFile.read().splitlines()
    poemProfileFile.close()

    for line in poemProfiles:
        if len(line) == 0 or line[0] == '#':
            continue

        profileFields = line.split(poemFileFieldDelimiter)

        profile = dict()
        for i in range(0, len(profileFieldNames)):
            profile[profileFieldNames[i]] = profileFields[i]

        #find server tree
        serverTree = dict()
        if profile['server'] in profileTree.keys():
            serverTree = profileTree[profile['server']]
        else:
            profileTree[profile['server']] = serverTree

        #find type tree
        typeTree = dict()
        if profile['service_flavour'] in serverTree.keys():
            typeTree = serverTree[profile['service_flavour']]
        else:
            serverTree[profile['service_flavour']] = typeTree

        #find type metric
        metricTree = dict()
        if profile['metric'] in typeTree.keys():
            metricTree = typeTree[profile['metric']]
        else:
            typeTree[profile['metric']] = metricTree

        #find vo tree
        vo = '-'
        if len(profile['vo']) > 0:
            vo = profile['vo']
        voTree = dict()
        if vo in metricTree.keys():
            voTree = metricTree[vo]
        else:
            metricTree[vo] = voTree

        #find fqan tree
        fqan = '-'
        if len(profile['fqan']) > 0:
            vfqan = profile['fqan']
        fqanTree = dict()
        if fqan in voTree.keys():
            fqanTree = voTree[fqan]
        else:
            voTree[fqan] = fqanTree

        voTree[fqan][profile['profile']] = profile

    return profileTree

def loadNameMapping(year, month, day):
    nameMappingDict = dict()

    nameMappingFile = None
    try:
        nameMappingFile = open('/etc/argo-egi-connectors/' + globopts['PrefilterPoemNameMapping'.lower()], 'r')
    except IOError:
        nameMappingFile = None

    if nameMappingFile != None:

        nameMappings = nameMappingFile.read().splitlines()
        nameMappingFile.close()

        for line in nameMappings:
            if len(line) == 0 or line[0] == '#':
                continue

            nameMappingFields = line.split(':')
            if len(nameMappingFields) > 1:
                oldName = nameMappingFields[0].strip()
                newName = nameMappingFields[1].strip()
                nameMappingDict[oldName] = newName

    return nameMappingDict

def getProfilesForConsumerMessage(profileTree, nameMapping, logItem):
    #chekc for nagios
    if logItem.get('monitoring_host') == None:
        return -1

    #find server tree
    if logItem['monitoring_host'] in profileTree.keys():
        serverTree = profileTree[logItem['monitoring_host']]
    else:
        return -1

    #find service type tree
    if logItem['service'] in serverTree.keys():
        typeTree = serverTree[logItem['service']]
    else:
        return -1

    #find type metric
    if logItem['metric'] in typeTree.keys():
        metricTree = typeTree[logItem['metric']]
    else:
        # check name mapping
        if logItem['metric'] in nameMapping:
            if nameMapping[logItem['metric']] in typeTree.keys():
                metricTree = typeTree[nameMapping[logItem['metric']]]
            else:
                return -1
        else:
            return -1

    #find vo tree
    if logItem.get('tags') != None and logItem.get('tags').get('vo') != None:
        if logItem['tags']['vo'] in metricTree.keys():
            voTree =  metricTree[logItem['tags']['vo']]
        else:
            return -1
    else:
        fqanList = list()
        for vo in metricTree.values():
            if '-' in vo.keys():
                fqan = vo['-']
                fqanList.extend(fqan.values())

        return fqanList

    # find vo_fqan tree
    if logItem.get('tags') != None and logItem.get('tags').get('vo_fqan') != None:
        if logItem['tags']['vo_fqan'] in voTree.keys():
            return voTree[logItem['tags']['vo_fqan']].values()
        else:
            if '-' in voTree.keys():
                return voTree['-'].values()
            else:
                return -1
    else:
        fqanList = list()
        for fqan in voTree.values():
            fqanList.extend(fqan.values())

        return fqanList


def prefilterit(reader, writer, ngis, profiles, nameMapping):
    num_falsemonhost, num_falseprofile, num_falseroc, num_msgs, rejected = 0, 0, 0, 0, 0
    for logItem in reader:
        num_msgs += 1
        if not logItem.get('monitoring_host'):
            num_falsemonhost += 1
        if logItem['monitoring_host'] in ngis.keys():
            ngiList = ngis[logItem['monitoring_host']]
            if 'ALL' in ngiList or (logItem['tags'] and logItem['tags']['roc'] in ngiList):
                #profile check
                msgprofiles = getProfilesForConsumerMessage(profiles, nameMapping, logItem)
                if type(msgprofiles) is int:
                    num_falseprofile += 1
                else:
                    if len(msgprofiles) > 0:
                        writer.append(logItem)
                    else:
                        num_falseprofile += 1
            else:
                num_falseroc += 1
        else:
            num_falsemonhost += 1

    return (num_msgs, num_msgs - num_falseroc - num_falsemonhost - num_falseprofile,
           num_falseroc + num_falsemonhost + num_falseprofile,
           num_falsemonhost, num_falseroc, num_falseprofile)


def main():
    parser = optparse.OptionParser(description="""Filters consumer messages based on various criteria
                                                    (allowed NGIs, service flavours, metrics...)""")
    parser.add_option('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str)
    group = optparse.OptionGroup(parser, 'Compute Engine usage')
    group.add_option('-d', dest='date', nargs=1, metavar='YEAR-MONTH-DAY')
    parser.add_option_group(group)
    group = optparse.OptionGroup(parser, 'Debugging usage')
    group.add_option('-f', dest='cfile', nargs=1, metavar='consumer_log_YEAR-MONTH-DAY.avro')
    parser.add_option_group(group)
    (options, args) = parser.parse_args()

    global logger
    logger = Logger(os.path.basename(sys.argv[0]))

    prefilter = {'Prefilter': ['ConsumerFilePath', 'PoemExpandedProfiles', 'PoemNameMapping', 'LookbackPoemExpandedProfiles']}
    schemas = {'AvroSchemas': ['Prefilter']}
    output = {'Output': ['Prefilter']}
    confpath = options.gloconf if options.gloconf else None
    cglob = Global(confpath, schemas, output, prefilter)
    global globopts
    globopts = cglob.parse()

    stats = ()

    if options.cfile and options.date:
        parser.print_help()
        raise SystemExit(1)
    elif options.cfile:
        fname = options.cfile
        date = options.cfile.split('_')[-1]
        date = date.split('.')[0]
        date = date.split('-')
    elif options.date:
        date = options.date.split('-')
    else:
        parser.print_help()
        raise SystemExit(1)

    if len(date) == 0 or len(date) != 3:
        logger.error('Consumer file does not end with correctly formatted date')
        parser.print_help()
        raise SystemExit(1)

    year, month, day = date

    # avro files
    if options.cfile:
        inputFile = options.cfile
    else:
        inputFile = gen_fname_repdate(logger, year+'-'+month+'-'+day, globopts['PrefilterConsumerFilePath'.lower()], '')
    outputFile = gen_fname_repdate(logger, year+'_'+month+'_'+day, globopts['OutputPrefilter'.lower()], '')

    try:
        schema = avro.schema.parse(open(globopts['AvroSchemasPrefilter'.lower()]).read())
        writer = DataFileWriter(open(outputFile, "w"), DatumWriter(), schema)
        reader = DataFileReader(open(inputFile, "r"), DatumReader())
    except IOError as e:
        logger.error(str(e))
        raise SystemExit(1)

    # load poem data
    ngis = loadNGIs(year, month, day)
    profiles = loadFilteredProfiles(year, month, day)
    nameMapping = loadNameMapping(year, month, day)

    s = time.time()
    msgs, msgswrit, msgsfilt, falsemonhost, falseroc, falseprofile = prefilterit(reader, writer, ngis, profiles, nameMapping)
    e = time.time()

    logger.info('ExecTime:%.2fs ConsumerDate:%s Read:%d Written:%d Filtered:%d(Monitoring_Host:%d,ROC:%d,ServiceTypes_Metrics:%d)' % (round(e - s, 2), year+'-'+month+'-'+day,
                                                                                    msgs, msgswrit, msgsfilt, falsemonhost, falseroc,
                                                                                    falseprofile))

    reader.close()
    writer.close()

main()

def extract_value(key, entry):
    if isinstance(entry, tuple):
        for e in entry:
            k, v = e[0]
            if key == k:
                return v
    else:
        return entry.get(key, None)


def build_map_endpoint_path(logger, bdiidata):
    mapping = dict()

    try:
        for entry in bdiidata:
            voname = extract_value('GlueVOInfoAccessControlBaseRule', entry)
            if isinstance(voname, list):
                voname = list(filter(lambda e: 'VO:' in e, voname))
                if voname:
                    voname = voname[0].split(':')[1]
                else:
                    continue
            sepath = extract_value('GlueVOInfoPath', entry)
            sepath = sepath[0] if isinstance(sepath, list) else None
            endpoint = extract_value('GlueSEUniqueID', entry['dn'].rdns)

            if voname and sepath and endpoint:
                mapping[endpoint] = {
                    'voname': voname,
                    'GlueVOInfoPath': sepath
                }
    except IndexError as exc:
        logger.error('Error building map of endpoints and storage paths from BDII data: %s' % repr(exc))
        logger.error('LDAP entry: %s' % entry)

    return mapping


def attach_sepath_topodata(logger, bdii_opts, bdiidata, group_endpoints):
    """
        Get SRM ports from LDAP and put them under tags -> info_srm_port
    """
    endpoint_sepaths = build_map_endpoint_path(logger, bdiidata)
    new_group_endpoints = list()

    for endpoint in group_endpoints:
        if endpoint['hostname'] in endpoint_sepaths:
            voname = endpoint_sepaths[endpoint['hostname']]['voname']
            sepath = endpoint_sepaths[endpoint['hostname']]['GlueVOInfoPath']
            endpoint['tags'].update({
                'vo_{}_attr_GlueVOInfoPath'.format(voname): sepath
            })
        new_group_endpoints.append(endpoint)

    return new_group_endpoints


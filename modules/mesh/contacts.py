def attach_contacts_topodata(logger, contacts, topodata):
    try:
        for entity in topodata:
            # group_groups topotype
            if 'subgroup' in entity:
                found_contacts = list(
                    filter(lambda contact:
                        contact['name'] == entity['subgroup'],
                        contacts)
                )
                if found_contacts:
                    emails = list()
                    for contact in found_contacts[0]['contacts']:
                        if isinstance(contact, str):
                            entity.update(notifications={
                                'contacts': found_contacts[0]['contacts'],
                                'enabled': True
                            })
                            break
                        else:
                            emails.append(contact['email'])
                    if emails:
                        entity.update(notifications={
                            'contacts': emails,
                            'enabled': True
                        })
            # group_endpoints topotype
            else:
                for contact in contacts:
                    fqdn, servtype = contact['name'].split('+')
                    emails = contact['contacts']
                    found_endpoints = list(
                        filter(lambda endpoint:
                               endpoint['hostname'] == fqdn \
                               and endpoint['service'] == servtype,
                               topodata)
                    )
                    if emails:
                        for endpoint in found_endpoints:
                            endpoint.update(notifications={
                                'contacts': emails,
                                'enabled': True
                            })

    except (KeyError, ValueError, TypeError) as exc:
        logger.warn('Error joining contacts and topology data: %s' % repr(exc))
        logger.warn('Topology entity: %s' % entity)
        logger.warn('Found contacts: %s' % found_contacts)

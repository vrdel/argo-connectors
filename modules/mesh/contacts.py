def filter_dups_noemails(contact_list):
    no_dups = set(contact_list)
    return list(no_dups)

def attach_contacts_topodata(logger, contacts, topodata):
    updated_topodata = list()

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
                                'contacts': filter_dups_noemails(found_contacts[0]['contacts']),
                                'enabled': True
                            })
                            break
                        else:
                            emails.append(contact['email'])
                    if emails:
                        entity.update(notifications={
                            'contacts': filter_dups_noemails(emails),
                            'enabled': True
                        })
            # group_endpoints topotype
            else:
                for contact in contacts:
                    fqdn, servtype = contact['name'].split('+')
                    emails = filter_dups_noemails(contact['contacts'])
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

            updated_topodata.append(entity)

        return updated_topodata

    except (KeyError, ValueError, TypeError) as exc:
        logger.warn('Error joining contacts and topology data: %s' % repr(exc))
        logger.warn('Topology entity: %s' % entity)
        logger.warn('Found contacts: %s' % found_contacts)

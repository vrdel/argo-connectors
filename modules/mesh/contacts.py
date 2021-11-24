def attach_contacts_topodata(logger, contacts, topodata):
    try:
        for entity in topodata:
            found_contacts = list(
                filter(lambda contact:
                    contact['name'] == entity['subgroup'],
                    contacts)
            )
            if found_contacts:
                emails = [ contact['email'] for contact in found_contacts[0]['contacts'] ]
                entity.update(notifications={
                    'contacts': emails,
                    'enabled': True
                })

    except (KeyError, ValueError) as exc:
        logger.warning('Error joining contacts and topology data: %s' % repr(exc))
        logger.warning('Topology entity: %s' % entity)

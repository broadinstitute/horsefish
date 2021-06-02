from zdesk import Zendesk

################################################################
## NEW CONNECTION CLIENT
################################################################
# Manually creating a new connection object
zendesk = Zendesk('https://yourcompany.zendesk.com', 'you@yourcompany.com', 'token', True)              

# If using an API token, you can create connection object using
# zendesk = Zendesk('https://yourcompany.zendesk.com', 'you@yourcompany.com', 'token', True)
# True specifies that the token is being used instead of the password

# See the zdeskcfg module for more sophisticated configuration at
# the command line and via a configuration file.
# https://github.com/fprimex/zdeskcfg

################################################################
## TICKETS
################################################################

# List
zendesk.tickets_list()

description = 'please reheat my coffee'

name = 'Test hOps_Python_code'

email = 'hOps@starbucks.com'

# Create
new_ticket = {
    'ticket': {
        'requester': {
            'name': name,
            'email': email,
        },
        'custom_fields': [
            {'id': 360012744452, 'value': 'question'},
            {'id': 360007369412, 'value': description},
            {'id': 360012744292, 'value': name},
            {'id': 360012782111, 'value': email},
            {'id': 360018545031, 'value': email}
        ],
        'subject':'Test for Python code',
        'description': description,
        'ticket_form_id': 360001770432,
        'tags': ['test', 'python'],
        'ticket_field_entries': [
            {
                'ticket_field_id': "(PO)Project Name",
                'value': 'Test Name'
            },
            {
                'ticket_field_id': 2,
                'value': '$10'
            }
        ]
    }
}

# Create the ticket and get its URL
result = zendesk.ticket_create(data=new_ticket)

# Need ticket ID?
from zdesk import get_id_from_url
ticket_id = get_id_from_url(result)

# Show
zendesk.ticket_show(id=ticket_id)

# # Delete
# zendesk.ticket_delete(id=ticket_id)
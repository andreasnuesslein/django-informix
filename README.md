# django-informix
A database driver for Django to connect to an Informix database

### settings.py
Django's settings.py require the following format for now:

    'connection': {
       'ENGINE': 'django_informix',
       'URL': 'jdbc:informix-sqli://<host>:<port>/<database>:INFORMIXSERVER=<informixserver>',
       'USER': '<username>',
       'PASSWORD': '<password>',
       'JARS': ['<path/to/lib/ifxjdbc.jar>'],
    },

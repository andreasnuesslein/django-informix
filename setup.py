from setuptools import setup

setup(
    name='django_informix',
    version='1.1',
    packages=['django_informix'],
    package_dir={'': 'src'},
    url='https://github.com/nutztherookie/django-informix',
    license='LGPL',
    author='Andreas Nüßlein',
    author_email='andreas.nuesslein@amnesty.de',
    install_requires=[
      'JayDeBeApi3 >= 1',
    ],
    description='Django Database connector for Informix',
    classifiers= [
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ]
)

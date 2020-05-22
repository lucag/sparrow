from os.path import join, dirname

from setuptools import setup, find_packages

README = join(dirname(__file__), 'src', 'sparrow', 'README.txt')

setup(
    name='sparrow',
    version='1.0b4',
    author='Jasper Op de Coul (Infrae)',
    author_email='jasper@infrae.com',
    description="Sparrow, Common RDF/SPARQL Database API",
    long_description=open(README).read() + open('HISTORY.txt').read(),
    classifiers=["Development Status :: 4 - Beta",
                 "Programming Language :: Python",
                 "License :: OSI Approved :: BSD License",
                 "Topic :: Software Development :: Libraries :: Python Modules",
                 "Environment :: Web Environment"],
    keywords='python RDF SPARQL',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    zip_safe=False,
    license='BSD',
    entry_points={
        'console_scripts': [
            'start_sesame_server = sparrow.sesame_backend:start_server',
            'configure_sesame = sparrow.sesame_backend:configure_server',
            'start_allegro_server = sparrow.allegro_backend:start_server'
        ]
    },
    install_requires=[
        'zope.interface',
        'simplejson',
        'httplib2',
        'lxml',
    ],
)

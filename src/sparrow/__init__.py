from sparrow.redland_backend import RedlandTripleStore
from sparrow.rdflib_backend import RDFLibTripleStore
from sparrow.sesame_backend import SesameTripleStore
from sparrow.allegro_backend import AllegroTripleStore

def database(backend, dburi):
    if backend == 'redland':
        db = RedlandTripleStore()
    elif backend == 'rdflib':
        db = RDFLibTripleStore()
    elif backend == 'sesame':
        db = SesameTripleStore()
    elif backend == 'allegro':
        db = AllegroTripleStore()
    else:
        raise ValueError('Unknown database backend: "%s"' % backend)
    db.connect(dburi)
    return db


from simpleservice.ormdb.tools.utils import init_database

from gogamechen3.models import TableBase


def init_gopdb(db_info):
    init_database(db_info, TableBase.metadata)

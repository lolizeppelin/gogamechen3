# -*- coding:utf-8 -*-
import os
from simpleutil.config import cfg
from simpleutil.log import log as logging

from simpleflow.api import load
from simpleflow.storage import Connection
from simpleflow.storage.middleware import LogBook
from simpleflow.engines.engine import ParallelActionEngine

from goperation.manager.rpc.agent import sqlite
from goperation.manager.rpc.agent.application.taskflow.application import AppFileUpgradeByFile
from goperation.manager.rpc.agent.application.taskflow.application import Application
from goperation.manager.rpc.agent.application.taskflow.database import DbUpdateFile
from goperation.manager.rpc.agent.application.taskflow.database import DbBackUpFile
from goperation.manager.rpc.agent.application.taskflow import pipe
from gogamechen3 import common
from gogamechen3.api.rpc.taskflow import GogameMiddle
from gogamechen3.api.rpc.taskflow import GogameDatabase
from gogamechen3.api.rpc.taskflow import GogameAppFile
from gogamechen3.api.rpc.taskflow import GogameAppBackupFile


CONF = cfg.CONF

LOG = logging.getLogger(__name__)


def upgrade_entitys(appendpoint,
                    objtype, objfiles,
                    entitys, timeline):
    upgradefile = None
    backupfile = None
    download_time = 600
    upzip_timeout = 600
    if common.APPFILE in objfiles:
        objfile = objfiles[common.APPFILE]
        md5 = objfile.get('md5')
        backup = objfile.get('backup', True)
        revertable = objfile.get('revertable', False)
        rollback = objfile.get('rollback', True)
        timeout = objfile.get('timeout')
        if timeout < download_time:
            download_time = timeout
        if timeout < upzip_timeout:
            upzip_timeout = timeout
        # 程序更新文件
        upgradefile = GogameAppFile(md5, objtype, rollback=rollback, revertable=revertable)
        if backup:
            # 备份entity在flow_factory随机抽取
            outfile = os.path.join(appendpoint.endpoint_backup,
                                   '%s.%s.%d.gz' % (objtype, common.APPFILE, timeline))
            # 程序备份文件
            backupfile = GogameAppBackupFile(outfile, objtype)

    applications = []
    middlewares = []
    _updates = {}
    for entity in entitys:
        if objtype != appendpoint._objtype(entity):
            raise ValueError('Entity not the same objtype')
        middleware = GogameMiddle(endpoint=appendpoint, entity=entity, objtype=objtype)
        middlewares.append(middleware)
        _database = []
        # 备份数据库信息
        for subtype in (common.DATADB, common.LOGDB):
            if subtype in objfiles:
                objfile = objfiles[subtype]
                md5 = objfile.get('md5')
                revertable = objfile.get('revertable', False)
                rollback = objfile.get('rollback', False)
                timeout = objfile.get('timeout')
                if timeout < download_time:
                    download_time = timeout
                dbinfo = appendpoint.local_database_info(entity, subtype)
                try:
                    update = _updates[subtype]
                except KeyError:
                    LOG.debug('New %s update file' % subtype)
                    update = DbUpdateFile(md5, revertable, rollback)
                    _updates[subtype] = update
                # 数据库备份文件
                backup = None
                if objfile.get('backup', False):
                    outfile = os.path.join(appendpoint.endpoint_backup,
                                           '%s.%d.%s.%d.gz' % (objtype, entity, subtype, timeline))
                    backup = DbBackUpFile(outfile)
                _database.append(GogameDatabase(backup=backup, update=update,
                                                timeout=timeout, **dbinfo))
        # 更新程序文件任务
        upgradetask = None
        if common.APPFILE in objfiles:
            upgradetask = AppFileUpgradeByFile(middleware, native=False,
                                               rebind=['upgradefile', 'upzip_timeout'])
        app = Application(middleware, upgradetask=upgradetask, databases=_database)
        applications.append(app)
    _updates.clear()

    book = LogBook(name='upgrade_%s' % appendpoint.namespace)
    store = dict(download_timeout=download_time, upzip_timeout=upzip_timeout)
    taskflow_session = sqlite.get_taskflow_session()
    upgrade_flow = pipe.flow_factory(taskflow_session, book,
                                     applications=applications,
                                     upgradefile=upgradefile,
                                     backupfile=backupfile,
                                     store=store)
    connection = Connection(taskflow_session)
    engine = load(connection, upgrade_flow, store=store,
                  book=book, engine_cls=ParallelActionEngine,
                  max_workers=4)
    e = None
    try:
        engine.run()
    except Exception as e:
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.exception('Upgrade task execute fail')
        else:
            LOG.error('Upgrade task execute fail, %s %s' % (e.__class__.__name__, str(e)))
    finally:
        connection.destroy_logbook(book.uuid)
    return middlewares, e

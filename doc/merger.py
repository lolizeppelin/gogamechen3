#!/usr/bin/python
# -*- encoding: utf-8 -*-
import sys
import logging

from simpleutil.config import cfg

from goperation.api.client import ManagerClient
from goperation.api.client.config import client_opts

from goperation.api.client.utils import prepare_results


from gogamechen3.api.client import GogameChen3DBClient

CONF = cfg.CONF


def client(session=None):
    return GogameChen3DBClient(httpclient=ManagerClient(url=CONF.gcenter, port=CONF.gcenter_port,
                                                        retries=CONF.retries, timeout=CONF.apitimeout,
                                                        token=CONF.trusted, session=session))


def merge(appfile, group, entitys, agent=None, databases=None):
    _client = client()

    body = {'appfile': appfile,
            'group_id': group,
            'entitys': entitys}
    if agent:
        body['agent_id'] = agent
    if databases:
        body['databases'] = databases

    code, result, data = prepare_results(_client.merge_entitys, body)
    if code:
        print('\033[1;31;40m')
        print 'Fail, code %d, result %s' % (code, result)
        if data:
            print data
        print('\033[0m')
        sys.exit(1)
    print('merge success')
    print(data)
    print('===========================')


def continue_merge(uuid):
    _client = client()
    code, result, data = prepare_results(_client.continue_merge, uuid)
    if code:
        print('\033[1;31;40m')
        print 'Fail, code %d, result %s' % (code, result)
        if data:
            print data
        print('\033[0m')
        sys.exit(1)
    print('continue success')
    print(data)
    print('===========================')


def main():
    logging.basicConfig(level=logging.WARN)
    CONF.register_cli_opts(client_opts)
    CONF(project='cmd')

    appfile = 'da27850e62bbd8301adfc6189602f659'

    group = 1
    entitys = []
    agent = None
    databases = {'gamedb': 2, 'logdb': 2}
    merge(appfile, group, entitys, agent=agent, databases=databases)

    #uuid = ''
    #continue_merge(uuid)


if __name__ == '__main__':
    main()

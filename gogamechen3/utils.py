import re
import psutil

from gogamechen3 import common

regx = re.compile('^[a-z][a-z0-9-]+?[a-z0-9-]$', re.IGNORECASE)


def find_process(procnames=None):
    if isinstance(procnames, basestring):
        procnames = [procnames, ]
    procnames = procnames or common.ALLTYPES
    pids = []
    for proc in psutil.process_iter(attrs=['pid', 'name', 'exe', 'username', 'cwd']):
        info = proc.info
        if info.get('exe') and info.get('name') in procnames:
            pids.append(dict(pid=info.get('pid'),
                             exe=info.get('exe'),
                             name=info.get('name'),
                             pwd=info.get('cwd'),
                             username=info.get('username')))
    return pids


def validate_string(value, lower=True):
    if not value:
        raise ValueError('String is empty')
    if not isinstance(value, basestring):
        raise ValueError('String is not basestring')
    if not re.match(regx, value):
        raise ValueError('String %s not match regx' % value)
    return value.lower() if lower else value
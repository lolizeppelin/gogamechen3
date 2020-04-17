from simpleutil.config import cfg
from gogamechen3.api.wsgi.config import register_opts

from gogamechen3 import common

CONF = cfg.CONF

register_opts(CONF.find_group(common.NAME))

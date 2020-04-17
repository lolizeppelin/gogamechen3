from simpleutil.config import cfg
def list_server_opts():
    from simpleservice.ormdb.config import database_opts
    from goperation.manager.wsgi.config import route_opts
    from gogamechen3.api.wsgi.config import resource_opts
    cfg.set_defaults(route_opts, routes=['gogamechen3.api.wsgi.routers'])
    return route_opts + resource_opts + database_opts


def list_agent_opts():
    from gogamechen3 import common
    group = cfg.OptGroup(common.NAME)
    CONF = cfg.CONF
    CONF.register_group(group)
    from goperation.manager.rpc.agent.config import rpc_endpoint_opts
    cfg.set_defaults(rpc_endpoint_opts, module='gogamechen3.api.rpc')
    return rpc_endpoint_opts

NAME = 'gogamechen3'

REGEXUSER = '[A-Za-z]+?[A-Za-z-_0-9]*?[A-Za-z0-9]'
REGEXPASS = '[a-zA-Z0-9-_.]'

GAMESERVER = 'gamesvr'
GMSERVER = 'gmsvr'
CROSSSERVER = 'publicsvr'
ALLTYPES = frozenset([GAMESERVER, GMSERVER, CROSSSERVER])

DATADB = 'datadb'
LOGDB = 'logdb'
APPFILE = 'appfile'

DELETED = -2  # has been delete
UNACTIVE = -1  # not active
OK = 0  # status ok
MERGEING = 1
SWALLOWING = 2
MERGEED = 4
MERGEFINISH = 5

POSTS_COUNT = {
    GAMESERVER: 2,
    GMSERVER: 2,
    CROSSSERVER: 1
}

APPAFFINITYS = {GAMESERVER: 1, CROSSSERVER: 2, GMSERVER: 4}
#    map app affinitys by bitwise operation
#    GM    CROSS  GAME
#     4      2      1
# GAME 1
# CROSS 2
# GM 4
# GM & CROSS 6
# GM & CROSS & GAME 7

DBAFFINITYS = {GAMESERVER: {DATADB: 1, LOGDB: 2},
               CROSSSERVER: {DATADB: 4}, GMSERVER: {DATADB: 8},}

#    map database affinitys by bitwise operation
#
#    GM-DATADB    CROSS-DATA-DB   GAME-LOGDB    GAME-DATADB
#         1             1              1            1
#         0             0              0            0
#
# GAME-DATADB 2**0 = 1
# GAME-LOGDB 2**1 = 2
# CROSS-DATA-DB = 2**2 = 4
# GM-DATADB = 2**3 = 8
#
#
# GAME-DATADB & GAME-LOGDB = 3
# CROSS-DATA-DB & GM-DATADB = 12
#
# affinity & DBAFFINITYS[GAMESERVER][DATADB]


# package static var

ENABLE = 1
DISABLE = 0

ANY = 'any'
ANDROID = 'android'
IOS = 'ios'

android = 1
ios = 2

PlatformTypeMap = {
    ANDROID: android,
    IOS: ios,
    ANY: ios | android,
}

#InvertPlatformTypeMap = dict([(v, k) for k, v in PlatformTypeMap.iteritems()])

SMALL_PACKAGE = 'small'
UPDATE_PACKAGE = 'update'
FULL_PACKAGE = 'full'
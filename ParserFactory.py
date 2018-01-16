from parsers.SSBParser import *
from parsers.BNPParibas import *
from parsers.BNYMellon import *

from abc import ABC


class ParserFactory(ABC):
    global logger
    logger = logging.getLogger(__name__)
    logging.config.fileConfig('%s' % CONF_LOGGING_INI, disable_existing_loggers=False)

    @staticmethod
    def factory(type, config):
        logger.debug("Get parser for type - %s" % type)
        parser = None
        if type == 'StateStreet':
            parser = SSBParser(config)
        elif type == 'BNPParibas':
            parser = BNPParibas(config)
        elif type == 'BNYMellon':
            parser = BNYMellon(config)

        return parser

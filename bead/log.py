'''
Logging infrastructure for bead.

Usage:
    from bead import log

    log.debug('Detailed information for debugging')
    log.info('General informational messages')
    log.warning('Warning messages')
    log.error('Error messages')
'''

import logging

# Single logger for all of bead
logger = logging.getLogger('bead')

# Export convenience functions
debug = logger.debug
info = logger.info
warning = logger.warning
error = logger.error

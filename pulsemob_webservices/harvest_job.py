# coding: utf-8

__author__ = 'jociel'

import logging
import ConfigParser
import solr
import argparse

from harvest import harvest
import solr_util
import time

logger = logging.getLogger(__name__)

config = ConfigParser.ConfigParser()
config.read('harvest.cfg')

solr_uri = config.get("harvest", "solr_uri")
solr_conn = solr.SolrConnection(solr_uri)


def _config_logging(logging_level='INFO', logging_file=None):

    allowed_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.setLevel(allowed_levels.get(logging_level, 'INFO'))

    if logging_file:
        hl = logging.FileHandler(logging_file, mode='a')
    else:
        hl = logging.StreamHandler()

    hl.setFormatter(formatter)
    hl.setLevel(allowed_levels.get(logging_level, 'INFO'))

    logger.addHandler(hl)

    return logger


def delete_article_entry(code):

    logger.info("Deleting: %s" % code)

    solr_conn.delete(code=code)


def add_update_article_entry(code, document, action, indexed_date):

    args = None

    try:
        args = solr_util.get_solr_args_from_article(document, indexed_date)
    except AttributeError as ex:
        logger.exception(ex)
    except ValueError as ex:
        logger.exception(ex)

    if not args:
        return

    while True:
        try:
            solr_conn.add(**args)
            break
        except Exception as ex:
            logger.error(
                "An error has occurred trying to access Solr. Arguments passed to Solr and the traceback are below:")
            logger.error(args)
            logger.exception(ex)
            logger.error("Sleeping for 1 minute to try again...")
            time.sleep(2)


def run():
    FORMAT = '%(asctime)-15s %(message)s'

    harvest(
        config.get("harvest", "article_meta_uri"),
        "article",
        config.get("harvest", "data_source_name"),
        "article_data",
        config.get("harvest", "pg_shared_folder_output"),
        config.get("harvest", "pg_shared_folder_input"),
        (add_update_article_entry, delete_article_entry)
    )

    solr_conn.commit()


def main():

    parser = argparse.ArgumentParser(
        description='Load data to Solr for the Pulsemob SciELO App'
    )

    parser.add_argument(
        '--logging_file',
        '-o',
        default=config.get("harvest", "logfile") or "harvester.log",
        help='Full path to the log file'
    )

    parser.add_argument(
        '--logging_level',
        '-l',
        default=config.get("harvest", "loglevel") or "DEBUG",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logggin level'
    )

    args = parser.parse_args()

    _config_logging(args.logging_level, args.logging_file)
    logger.info('Loading data for the Pulsemob SciELO APP')

    run()


if __name__ == '__main__':

    main()

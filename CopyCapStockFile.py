import shutil
import sys
import os
import glob
import logging.handlers
import logging.config

from datetime import datetime


def main(argv=None):
    if argv is None:
        argv = sys.argv

    logger = logging.getLogger('SubAdvisedProcessor - ' + __name__)
    logging.config.fileConfig('../conf/logging.ini', disable_existing_loggers=False)

    src_data_dir = str(argv[1]).rstrip()
    folder_name = os.path.basename(os.path.dirname(src_data_dir))
    dest_dir = str(argv[2]).rstrip()

    file_pattern = '*'
    if len(argv) > 3:
        file_pattern = str(argv[3]).rstrip()

    process_date = datetime.today().strftime("%Y%m%d")
    processed_fileName = '../data/processed_' + process_date + '.txt'

    logger.debug("\nSrc Directory - %s\n Dest Directory - %s\n process date - %s\n processed tracker filename - %s" % (
    folder_name, dest_dir, process_date, processed_fileName))

    for filename in glob.glob1(src_data_dir, file_pattern):
        file_name = os.path.basename(filename)
        logger.debug("Processing file with path name - %s" % filename)
        logger.debug("Processing file - %s" % file_name)
        if file_name in open(processed_fileName, 'r+').read():
            print("File %s already processed. Skipping to next file." % filename)
            continue
        else:
            print('Copying %s to %s' % (filename, dest_dir))
            shutil.copy2(src_data_dir + filename, dest_dir + folder_name + '_' + filename)


if __name__ == '__main__':
    sys.exit(main())

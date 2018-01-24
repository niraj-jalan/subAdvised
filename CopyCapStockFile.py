import shutil
import sys
import os
import glob
from datetime import datetime


def main(argv=None):
    if argv is None:
        argv = sys.argv

    src_data_dir = str(argv[1]).rstrip()
    dest_dir = str(argv[2]).rstrip()
    process_date = datetime.today().strftime("%Y%m%d")
    processed_fileName = '../data/processed_' + process_date + '.txt'
    for filename in glob.glob1(src_data_dir, '*' + process_date):
        file_name = os.path.basename(filename)
        if file_name in open(processed_fileName, 'r+').read():
            print("File %s already processed. Skipping to next file." % filename)
            continue
        else:
            print('Copying %s to %s' % (file_name, dest_dir))
            shutil.copy2(file_name, dest_dir)


if __name__ == '__main__':
    sys.exit(main())

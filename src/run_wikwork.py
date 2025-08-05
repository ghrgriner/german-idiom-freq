#    Example code for running `wikwork` package to get lemma main form.
#    Copyright (C) 2025 Ray Griner (rgriner_fwd@outlook.com)

"""Example code for running `wikwork` package to get lemma main form.

This doesn't use the wrapper in the `wikwork` package to retrieve data
on all words in the input file and output at the end. Instead, it outputs
each word one-at-a-time so that if there is an error retrieving a page (e.g.,
a network timeout) we do not have to start over or rely on a cached file).

In this example, only the lemma main form is saved to the output file (of the
data in the wikitext).
"""

import csv
import logging
import os

from wikwork import io_options, german

logger = logging.getLogger('wikwork')
logger.setLevel(logging.INFO)

fh = logging.FileHandler('get_lemma_main_form.log', mode='w')
ch = logging.StreamHandler()

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

#------------------------------------------------------------------------------
# Parameters
#------------------------------------------------------------------------------
INPUT_WORDS_FILENAME = 'input_lvw.txt'
HEADWORD_LANG_CODE = 'de'
INPUT_WORDS_COLUMN_NAME = 'Word'
OUTPUT_WORDS_FILENAME = 'output_lvw.txt'
# After every API call, the program sleeps for this amount of seconds. The
# 0.72 value assumes the user has an access token of 3600 calls/hr.
SLEEP_TIME = 0.72
CACHE_DIR = 'cache_dir_lvw'
# These are passed in the request headers. It might be possible to run this
# without an access token, but the permitted rate is lower than the rate with
# a non-anonymous token.
access_token = os.environ['MEDIAWIKI_TOKEN']
user_email = os.environ['USER_EMAIL']

#------------------------------------------------------------------------------
# Main entry point
#------------------------------------------------------------------------------

if not user_email:
    raise ValueError('USER_EMAIL environment variable is empty or not set')

my_headers = {
    'Authorization': f'Bearer {access_token}',
    'User-Agent': f'(wikwork package) {user_email}',
}

io_opts = io_options.IOOptions(
    output_dir='clfm',
    project='Wiktionary',
    cache_mode = io_options.CacheMode.NO_READ_OR_WRITE,
    sleep_time = SLEEP_TIME,
    headers=my_headers)
io_options = io_opts

logger = logging.getLogger(__name__)

input_list = []
with open(INPUT_WORDS_FILENAME, encoding='utf-8', newline='') as f:
    reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
    input_list = [ row[INPUT_WORDS_COLUMN_NAME] for row in reader]

logger.info('Processing %d words from %s in %s.wiktionary.org',
            len(input_list), INPUT_WORDS_FILENAME, HEADWORD_LANG_CODE)

with open(OUTPUT_WORDS_FILENAME, 'w', encoding='utf-8', newline='') as csvfile:
    outwriter = csv.writer(csvfile, delimiter='\t', lineterminator='\n',
                           quoting=csv.QUOTE_MINIMAL)

    # additional variables in the subclass for printing
    empty_entry = german.GermanEntry()

    # Write header (headword, status_msg, revision,
    #   filename1, prob_license1, ...)
    flathead = ['headword','status_msg','revision','timestamp']
    publicvars = [ var for var in vars(empty_entry).keys()
                   if var[0] != '_' and var == 'lemma_main_form' ]
    flathead.extend( colnm + '_1' for colnm in publicvars )
    tuphead = ['filename','prob_licenses','prob_authors',
               'prob_attribs', 'revision', 'download_status']
    outwriter.writerow(flathead)

    for i, word in enumerate(input_list):
        word_info = german.GermanWord(headword=word,
                            lang_code=HEADWORD_LANG_CODE)

        if word_info.valid_input:
            word_info.fetch_revision_info(io_options=io_options)
        word_info.fetch_word_page(io_options=io_options)

        if ((i+1) % 20) == 0:
            print(f'File: {INPUT_WORDS_FILENAME}: Processed word {i+1}')

        # Write the data
        #for row in headword_list:
        row = word_info
        flattened = [row.headword, row.status_msg, row.revision,
                     row.timestamp]
        if row.entries:
            for colnm in publicvars:
                val = getattr(row.entries[0],colnm)
                if isinstance(val, list):
                    flattened.append('; '.join(val))
                else:
                    flattened.append(val)
        else:
            flattened.extend('' for colnm in publicvars)
        outwriter.writerow(flattened)
        csvfile.flush()

#    Example code for running `wikwork` package to get Wiktionary wikitext
#    Copyright (C) 2025 Ray Griner (rgriner_fwd@outlook.com)

"""Example code for running `wikwork` package to get Wiktionary wikitext

The wikitext files will be in wikwork_output/wtxt.

The package will skip about ten of the headwords because it will skip
page names with periods and quotation marks. These are the pages with
revision=0 in the output text file.

The wikitext from these pages can be downloaded manually by going to the
page editor on the Wiktionary web site.

To get the Lemmaverweis.txt file for use with post_process.py,
create a Lemmaverweis subdirectory in the current directory and then
change to the wikwork_output/wtxt subdirectory and run:

> grep Lemmaverweis *.txt > ../../Lemmaverweis/Lemmaverweis.txt

On Windows, you should be able to open a Windows Powershell and run (from
within the directory):
> sls Lemmaverweis *.txt > ../../Lemmaverweis/Lemmaverweis.txt

"""

import logging

from wikwork import wrapper, io_options
import os

#------------------------------------------------------------------------------
# Set up logger, although will generate a lot of info messages that can be
# ignored because it parses a lot of the wikitext but will warn when it
# encounters wikitext templates that it doesn't know how to handle. These
# warnings will mention the 'Lemmaverweis' template we are interested in, but
# that's ok since we will not use the parsed output.
#------------------------------------------------------------------------------
logger = logging.getLogger('wikwork')
logger.setLevel(logging.INFO)

fh = logging.FileHandler('run_wikwork.log', mode='w')
ch = logging.StreamHandler()

# create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

#------------------------------------------------------------------------------
# Main entry point
#------------------------------------------------------------------------------

# The first line should be 'headword' (or whatever you set the parameter
# `input_words_column_name` to in `wrapper.words_wrapper`), followed by the
# headwords.
input_file = 'wikwork_input.txt'

# Users will need to set up these environment variables, review Mediawiki
# terms of use and get an access token. These are only used when making
# my_headers. The sleep time is calibrated assuming a token with a limit of
# 5000 Rest API calls per hour. Users
access_token = os.environ['MEDIAWIKI_TOKEN']
user_email = os.environ['USER_EMAIL']
sleep_time = 0.8

#    'Authorization': f'Bearer {access_token}',
my_headers = {
    'Authorization': f'Bearer {access_token}',
    'User-Agent': f'(wikwork package) ({user_email})',
}
del user_email
del access_token

io_opts = io_options.IOOptions(
    output_dir='wikwork_output',
    project='Wiktionary',
    cache_mode = io_options.CacheMode.READ_AND_WRITE,
    headers=my_headers)

io_opts.audio_out_mode=io_options.AudioOutMode.NO_OVERWRITE

res = wrapper.words_wrapper(
    input_words_filename=f'{input_file}',
    headword_lang_code='de',
    audio_html_lang_code='de',
    io_options=io_opts,
    input_words_column_name='headword',
    fetch_word_page=True,
    output_words_filename='wikwork_output.txt',
)


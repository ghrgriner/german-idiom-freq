#    Print matches (all or a sample) for selected idioms for manual review.
#    Copyright (C) 2025 Ray Griner (rgriner_fwd@outlook.com)
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <https://www.gnu.org/licenses/>.
#------------------------------------------------------------------------------

'''Print matches (all or a sample) for selected idioms for manual review. 

This reads a file of all matches for selected idioms that was created using
the `match_file` parameter of `count_regexes`.

It also reads a file with a variable `n_manual_sampsize` that takes value
'all' or an integer sample size. For each idiom in both files it prints two
of three files:

{idiom}.txt - All the matches for the idiom
{idiom}_sample.txt - The sample of the matches, if a sample was requested
  and the requested size is less than the starting number of matches
{idiom}_fprep.txt - A file that is {idiom}_sample.txt (if it was created)
  or {idiom}.txt otherwise. The idea is that this is the file the user will
  modify during the review. For example, when reviewing, we rename this
  file to {idiom}_final.txt and then delete lines that are not matches. We
  do not modify {idiom}.txt or {idiom}_sample.txt during the review.

In the above filenames, `idiom` is just the `headword` with underscores
replacing spaces.

'''

import csv
import os

import pandas as pd

#------------------------------------------------------------------------------
# Parameters
#------------------------------------------------------------------------------
IDIOM_STOP = None
IDIOM_FILE = os.path.join('input', 'dewk_redewendungen_and_regex.txt')
SAMPLE_CONFIG_FILE = os.path.join('input', 'sample_config.txt')
MATCH_FILE = os.path.join('samp_output', 'match_file.txt')
OUTPUT_DIR = 'manrev'

#------------------------------------------------------------------------------
# Main entry point
#------------------------------------------------------------------------------
if IDIOM_STOP is None:
    idiom_rows = None
else:
    idiom_rows = IDIOM_STOP - 1
idiom_df = pd.read_csv(IDIOM_FILE, sep='\t', na_filter=False,
                       quoting=csv.QUOTE_NONE, nrows=idiom_rows)

idiom_df['ID'] = idiom_df['orig order']

sd_df = pd.read_csv(SAMPLE_CONFIG_FILE, sep='\t', na_filter=False,
                       quoting=csv.QUOTE_NONE)
sd_df = sd_df[['headword','n_manual_sampsize']]
idiom_df = idiom_df.merge(sd_df, left_on='headword', right_on='headword')
idiom_df = idiom_df[idiom_df.n_manual_sampsize != '']

match_df = pd.read_csv(MATCH_FILE, sep='\t', header=None,
                       names=['headword','text'], quoting=csv.QUOTE_NONE)
match_df = match_df.sort_values(['headword','text'])

def process_idiom(headword, n_manual_sampsize, note_id):
    file_prefix = headword.replace(' ','_')
    matches = match_df[match_df.headword == headword][['text']]
    matches.to_csv(os.path.join(OUTPUT_DIR, f"{file_prefix}.txt"),
                   sep='\t', header=False, index=False,
                   quoting=csv.QUOTE_NONE)

    if n_manual_sampsize != 'all' and int(n_manual_sampsize) <= len(matches):
        result = matches.sample(n=int(n_manual_sampsize),
                                random_state=3293 + int(note_id))
        result.to_csv(os.path.join(OUTPUT_DIR, f"{file_prefix}_sample.txt"),
                      sep='\t', header=False, index=False,
                      quoting=csv.QUOTE_NONE)
    else:
        result = matches
    result.to_csv(os.path.join(OUTPUT_DIR, f"{file_prefix}_fprep.txt"),
                  sep='\t', header=False, index=False,
                  quoting=csv.QUOTE_NONE)

for row in idiom_df[['headword','n_manual_sampsize','orig order']].values:
    process_idiom(headword=row[0], n_manual_sampsize=row[1], note_id=row[2])


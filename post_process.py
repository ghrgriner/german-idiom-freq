#    Add manual results and link to page.
#    Copyright (C) 2025 Ray Griner (rgriner_fwd@outlook.com)

'''Add manual results and link to page. Compare counts within groups.

This uses and passes through the 'main_form' and 'related_headword' variables
in the input file. It generates three listings for each of these variables:

1. Cases where we have selected a canonical form which gives a count for the
group <= the count for the group defined using the Wiktionary main form.

2. The count for the group defined using the Wiktionary main form may be
ill-defined, as we might have defined a group where Wiktionary has assigned
different main forms to the entries in our group. Therefore, we print a
listing when this occurs.

3. Print records and raise exception where:
   a) The `n_final` for an item in a group is at least five more than the
      `n_final` on the canonical form we selected for the group.
   b) The headword for the item is not in an OK-list that we checked to confirm
      that we are ok with exceeding the threshold of five. See the comments
      by the `OK_LIST` definition for the reasons we are okay with these
      assignments.

'''

#------------------------------------------------------------------------------
# File: post_process.py
# Desc: Add manual results and link to page. Compare counts within groups by
#   `main_form`.
#------------------------------------------------------------------------------
import csv
import re
import urllib

import pandas as pd
import numpy as np

#------------------------------------------------------------------------------
# Parameters
#------------------------------------------------------------------------------
COUNTS_FILE = 'tmp_output/dewk_redewendungen_counts.txt'
MANUAL_FILE = 'manual/dewk_redewendungen_v1.txt'
OUTPUT_FILE = 'output/dewk_redewendungen_final.txt'
# Don't give warnings when these headwords have more hits than the main form.
# For the first two, they are a subset of the words in the main form so of
# course they will have more hits, but the main form is more freq than the
# form with just the subset by a large amount, or both counts are low.
# For 'am Boden sein' the counts for this and 'am Boden zerstört sein' are
# both large, n=54 (excluding 'zerstört') and n=50, respectively, so choose
# 'am Boden sein' since it seems to also include the latter.
# For 'mit gespalterner Zunge', the form with 'reden' is more common when
# allowing capital letters for the verb, which we will likely eventually make
# the default behavior.
# For 'mit etwas hinterm Berg halten', there are 7 hits for this and 5 for
# 'hinterm Berg' but the latter regex also includes 'hinter dem' in the search;
# this will likely be changed in the future.
OK_LIST = ['ein Herz fassen','sich gerädert fühlen','etwas in Grenzen halten',
           'am Boden zerstört sein',
           'mit gespaltener Zunge sprechen',
           'mit etwas hinterm Berg halten',
           'Feuer fangen', 'in Flammen stehen']

def hw_to_title(x):
    title = urllib.parse.quote(x.replace(' ','_'))
    return f'https://de.wiktionary.org/wiki/{title}'

def check_main_form_most_freq(df_, var):
    df=df_.copy()

    #--------------------------------------------------------------------------
    # 1. Add n_final_man to input dataset. This assigns to every record the
    #    n_final for the main form of that record.
    # Also create `cf_man`, the main form assigned manually in this project.
    #--------------------------------------------------------------------------
    mainform_df = df[  (df[var] == '')
                     | (df[var] == df.headword)].copy()
    mainform_df['n_final_man'] = mainform_df.n_final
    df['cf_man'] = np.where(df[var] =='', df.headword, df[var])
    df = df.merge(mainform_df[['headword','n_final_man']],
                  how='left', left_on='cf_man', right_on='headword',
                  validate='m:1')
    del mainform_df

    #-------------------------------------------------------------------------
    # 2. See if there was a main form explicitly assigned in deWK and if
    # so, assign it to all the groups defined by `cf_man`. This variable is
    # named `mf_wk` and also create `n_final_wk` which is the n associated
    # with this main form.
    #-------------------------------------------------------------------------
    #df = df.merge(lvw_df['dewk_main_form'], how='left',
    #              left_on='headword_x', right_index=True)
    #df['mf_wk'] = df.dewk_main_form.fillna('')
    df['mf_wk'] = df.dewk_main_form_on_variant
    all_in_group = df[df.mf_wk != ''][['cf_man','mf_wk']].drop_duplicates()

    dup_key = all_in_group.cf_man.duplicated()
    if dup_key.any():
        print(f'\n{var}: Wiktionary Assigns > 1 Main Form to a Group')
        print(all_in_group[all_in_group.cf_man.isin(
                                 all_in_group.cf_man[dup_key])])
    all_in_group = all_in_group[~all_in_group.cf_man.duplicated()]
    all_in_group = all_in_group.merge(df[['headword_x','n_final']],
                     how='left', left_on='mf_wk', right_on='headword_x')
    all_in_group['n_final_wk'] = all_in_group.n_final
    df = df.merge(all_in_group[['cf_man','mf_wk','n_final_wk']],
                  how='left',left_on='cf_man', right_on='cf_man',
                  validate='m:1')
    df['mf_wk'] = df.mf_wk_y.fillna('')

    # Listing 2
    listing2 = ((df[var] != '') &
                (df.mf_wk != '') &
                (df.n_final_man.astype(float) <=
                 df.n_final_wk.astype(float)) &
                (df[var] != df.mf_wk))
    local_le_dewk = df[listing2]

    if listing2.any():
        print(f'\n{var}: Our canonical form has count <= that of Wiktionary'
               ' main form')
        print(local_le_dewk[['headword_x', var,'mf_wk',
                          'n_final','n_final_man','n_final_wk']])
    local_le_dewk[['headword_x', var,'mf_wk',
                   'n_final','n_final_man','n_final_wk']].to_csv(
                     f'output/{var}_not_using.txt')

    listing3 = ((df.n_final != '') & (df.n_final_man != '')
         & (~df.headword_x.isin(OK_LIST))
         & (df.n_final.astype(float) >= 5 + df.n_final_man.astype(float)))
    if listing3.any():
        print(df[listing3][['headword_x','cf_man','n_final', 'n_final_man']])
        raise ValueError(f'{var}: Frequency of canonical form is less than'
                          'frequency of group item by more than 5')

LVW_DICT = {}
def add_to_lvw_dict(x):
    # x should be of the form:
    #  'head_word.de.[0-9]+.txt:{{Lemmaverweis|main form}}'
    # where 'head_word' is just the headword for the page with underscores
    # replacing spaces. See `run_wikwork.py` for instructions for creating
    # the text files.
    x_list = x.split(':')
    end_re = re.compile(r'\.de\..*$')
    nebenform = end_re.sub('', x_list[0]).replace('_', ' ')
    mainform = x_list[1].replace('{{Lemmaverweis|','').replace('}}','')
    if nebenform in LVW_DICT:
        raise ValueError(f'{nebenform=} in dict twice')
    else:
        LVW_DICT[nebenform] = mainform

def read_lemmaverweis():
    lvw_df = pd.read_csv('Lemmaverweis/Lemmaverweis.txt', header=None,
                   sep='\t', quoting=csv.QUOTE_NONE, names=['Value'])
    lvw_df.Value.map(add_to_lvw_dict)

#------------------------------------------------------------------------------
# Main Entry Point
#------------------------------------------------------------------------------
#lvw_df = read_lemmaverweis()
counts_df = pd.read_csv(COUNTS_FILE, sep='\t', quoting=csv.QUOTE_NONE,
                usecols=['Redewendung','orig order','re1','re2',
                         'n_cum_1','n_seq_1','n_ic_cum_1','n_ic_seq_1',
                         'n_cum_2','n_seq_2','n_ic_cum_2','n_ic_seq_2'],
                dtype=str, keep_default_na=False,na_values=[])

# users running this code after downloading from the repository will not have
# access to the 'Lemmaverweis.txt' file but can just read in the
# dewk_main_form_on_variant form from the uploaded output file and merge
# it to counts_df. Users that want to create 'Lemmaverweis.txt' themselves
# can follow the instructions in `run_wikwork.py`.

read_lemmaverweis()
counts_df['dewk_main_form_on_variant'] = counts_df.Redewendung.map(
    lambda x: LVW_DICT.get(x, ''))

counts_df['sort_order'] = counts_df['orig order']
counts_df['id'] = counts_df['orig order']
manual_df = pd.read_csv(MANUAL_FILE, sep='\t', quoting=csv.QUOTE_NONE,
                usecols=['headword','n_manual','n_manual_cmt','main_form',
                         'related_headword'],
                dtype=str, keep_default_na=False,na_values=[])
counts_df = counts_df.merge(manual_df, left_on='Redewendung',
                            right_on='headword', validate='1:1')
counts_df['n_final'] = np.where(counts_df.n_manual != '',
                                counts_df.n_manual, counts_df.n_cum_1)
counts_df['link'] = counts_df.headword.map(hw_to_title)
check_main_form_most_freq(counts_df, var='main_form')
check_main_form_most_freq(counts_df, var='related_headword')
outvars=['headword','sort_order','id','re1','re2',
         'n_cum_1','n_seq_1','n_ic_cum_1','n_ic_seq_1',
         'n_cum_2','n_seq_2','n_ic_cum_2','n_ic_seq_2',
         'n_manual','n_manual_cmt','n_final','main_form',
         'related_headword','dewk_main_form_on_variant','link']
counts_df[outvars].to_csv(OUTPUT_FILE, sep='\t', quoting=csv.QUOTE_NONE,
                          index=False)

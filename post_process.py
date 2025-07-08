#    Add manual results and link to page.
#    Copyright (C) 2025 Ray Griner (rgriner_fwd@outlook.com)

'''Add manual results and link to page. Compare counts within groups.'''

#------------------------------------------------------------------------------
# File: post_process.py
# Desc: Add manual results and link to page. Compare counts within groups by
#   `Hauptform`.
#------------------------------------------------------------------------------
import csv
import urllib

import pandas as pd
import numpy as np

#------------------------------------------------------------------------------
# Parameters
#------------------------------------------------------------------------------
COUNTS_FILE = 'tmp_output/dewk_redewendungen_counts.txt'
MANUAL_FILE = 'manual/dewk_redewendungen_v1.txt'
OUTPUT_FILE = 'output/dewk_redewendungen_final.txt'
# Don't give warnings when these Redewendung have more hits than the Hauptform.
# For the first two, they are a subset of the words in the Hauptform so of
# course they will have more hits, but the Hauptform is more freq than the
# form with just the subset.
# For 'mit gespalterner Zunge', the form with 'reden' is more common when
# allowing capital letters for the verb, which we will likely eventually make
# the default behavior.
# For 'mit etwas hinterm Berg halten', there are 7 hits for this and 5 for
# 'hinterm Berg' but the latter regex also includes 'hinter dem' in the search;
# this will likely be changed in the future.
OK_LIST = ['ein Herz fassen','sich gerädert fühlen',
           'mit gespaltener Zunge sprechen',
           'mit etwas hinterm Berg halten']

def hw_to_title(x):
    title = urllib.parse.quote(x.replace(' ','_'))
    return f'https://de.wiktionary.org/wiki/{title}'

def check_main_form_most_freq(df_):
    df=df_.copy()
    mainform_df = df[  (df.Hauptform == '')
                     | (df.Hauptform == df.Redewendung)].copy()
    mainform_df['n_main'] = mainform_df.n_final
    df['main_red'] = np.where(df.Hauptform=='', df.Redewendung, df.Hauptform)
    df = df.merge(mainform_df[['Redewendung','n_main']],
                  how='left', left_on='main_red', right_on='Redewendung',
                  validate='m:1')
    df['prob'] = ((df.n_final != '') & (df.n_main != '')
                  & (~df.Redewendung_x.isin(OK_LIST))
                  & (df.n_final.astype(float) > df.n_main.astype(float)))
    if df.prob.any():
        print(df.columns)
        print(df[df.prob][['Redewendung_x','Hauptform','n_final','n_main']])
        raise ValueError('Records found where Hauptform not most freq form')

#------------------------------------------------------------------------------
# Main Entry Point
#------------------------------------------------------------------------------
counts_df = pd.read_csv(COUNTS_FILE, sep='\t', quoting=csv.QUOTE_NONE,
                usecols=['Redewendung','orig order','re1','re2',
                         'n_cum_1','n_seq_1','n_ic_cum_1','n_ic_seq_1',
                         'n_cum_2','n_seq_2','n_ic_cum_2','n_ic_seq_2'],
                dtype=str, keep_default_na=False,na_values=[])
counts_df['sort_order'] = counts_df['orig order']
counts_df['id'] = counts_df['orig order']
manual_df = pd.read_csv(MANUAL_FILE, sep='\t', quoting=csv.QUOTE_NONE,
                usecols=['Redewendung','n_manual','n_manual_cmt','Hauptform'],
                dtype=str, keep_default_na=False,na_values=[])
counts_df = counts_df.merge(manual_df, left_on='Redewendung',
                            right_on='Redewendung', validate='1:1')
counts_df['n_final'] = np.where(counts_df.n_manual != '',
                                counts_df.n_manual, counts_df.n_cum_1)
counts_df['link'] = counts_df.Redewendung.map(hw_to_title)
check_main_form_most_freq(counts_df)
outvars=['Redewendung','sort_order','id','re1','re2',
         'n_cum_1','n_seq_1','n_ic_cum_1','n_ic_seq_1',
         'n_cum_2','n_seq_2','n_ic_cum_2','n_ic_seq_2',
         'n_manual','n_manual_cmt','n_final','link']
counts_df[outvars].to_csv(OUTPUT_FILE, sep='\t', quoting=csv.QUOTE_NONE,
                          index=False)


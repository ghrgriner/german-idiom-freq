#    Create basic descriptive statistics and listings.
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

'''Create basic descriptive statistics and listings.
'''

import csv
import math
import os

import numpy as np
import pandas as pd

#------------------------------------------------------------------------------
# Parameters
#------------------------------------------------------------------------------
RESULTS_FILE = os.path.join('output', 'redewendungen_final.txt')
REPORTING_DIR = 'reporting'
N_FOR_LISTING = 50

#------------------------------------------------------------------------------
# Functions
#------------------------------------------------------------------------------
def label_cat(cat):
    if cat in [-1, 0]:
        return str(cat+1)
    else:
        return str(2**cat) + '-' + str(2**(cat+1)-1)

#------------------------------------------------------------------------------
# Main entry point
#------------------------------------------------------------------------------
df = pd.read_csv(RESULTS_FILE, sep='\t', na_filter=False,
                 dtype={'n_final': int},
                 quoting=csv.QUOTE_NONE)
df['is_main_form'] = (df.main_form == '') | (df.main_form == df.headword)
df['n_final_cat'] = df.n_final.map(
                          lambda x: -1 if not x else int(math.log2(x)))
df['n_final_label'] = df.n_final_cat.map(label_cat)
df['blank'] = ''
freq_df = pd.DataFrame(df.groupby(['n_final_cat','n_final_label']).size(),
                       columns=['freq'])
freq_mf_df = pd.DataFrame(df[df.is_main_form].groupby(
                       ['n_final_cat','n_final_label']).size(),
                       columns=['freq_mf'])
freq_df = freq_df.merge(freq_mf_df, left_index=True, right_index=True,
                        validate='1:1')
freq_df = freq_df.sort_index(level=0, ascending=False)
freq_df = freq_df.droplevel(level=0)
freq_df['cum_freq'] = freq_df.freq.cumsum()
freq_df['cum_freq_mf'] = freq_df.freq_mf.cumsum()
freq_df.at['Total','freq'] = freq_df.freq.sum()
freq_df.at['Total','freq_mf'] = freq_df.freq_mf.sum()
freq_df['blank'] = ''
freq_df = freq_df.reset_index(names=['label'])
freq_df[['blank','label','freq','cum_freq','freq_mf','cum_freq_mf','blank']
       ].to_csv(os.path.join(REPORTING_DIR, 'table1.txt'),
                float_format='%.0f',
                index=False, sep='|', quoting=csv.QUOTE_NONE)

df = df.sort_values(['n_final'], ascending=False)
in_neither = (df.link_de == '') & (df.link_en == '')
if in_neither.any():
    print(df[in_neither][['headword']])
    raise ValueError('Some idioms are reported as not in de or en Wiktionary')

df['source'] = np.where((df.link_de != '') & (df.link_en != ''),
                        'de+en', np.where(df.link_de != '', 'de', 'en'))

out_df = df.head(N_FOR_LISTING)[['blank','headword','source','n_final',
                                 'n_manual_cmt','main_form','blank']]
out_df.to_csv(os.path.join(REPORTING_DIR, 'listing.txt'),
                           sep='|', quoting=csv.QUOTE_NONE,
                           index=False)

print(out_df)

#print(df)

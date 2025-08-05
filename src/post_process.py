#    Add manual results and links to pages. Compare counts within groups.
#    Copyright (C) 2025 Ray Griner (rgriner_fwd@outlook.com)

'''Add manual results and links to pages. Compare counts within groups.

The program does the following:

1. It merges the counts from manual review with the automatically generated
   counts from the regular expressions. These are in two different input
   files so that we can update the manual review results without needing
   to update the automatically generated counts. The relevant variables
   are `n_manual` (from input), `n_manual_cmt` (from input) and `n_final`
   (`=max(n_manual, n_cum_1)`, where `n_cum_1` is the automatic count.

   [NOTE: all the action described on this point is temporarily disabled as
   manual review is not yet performed.]

2. It creates `de_link` and `en_link`, which are the links to the German-
   language and English-language Wiktionary pages, respectively.

3. It creates `dewk_main_form_on_variant` based on the value of the
   `Lemmaverweis` tag on the German-language Wiktionary. Its use is
   discussed further in the next point.

4. It checks whether the format in `n_manual_cmt` is standardized and
   cross-checks the values in this field against `n_manual` and `n_cum_1`.
   See `check_comment_math` for details.

5. It merges on the `main_form` and `related_headword` variables from the
   `MANUAL_FILE` and creates a number of listings to review the assignment.
   These fields are used to define groupings of the idioms and to select
   one idiom as the canonical form in the group. Usually we would like the
   canonical form to be the most common idiom, but this criteria may not be
   sufficient to distinguish between the competing choices or there may be
   only a slight difference in counts and we consider another factor to be
   important.
   Other factors considered include:
   - Preference is given to idioms listed in the German-language Wiktionary
     (as opposed to only the English-language Wiktionary)
   - Preference is given to idioms that give the direct object ('etwas',
     'jemandem', etc.) in the headword when the idiom usually uses such an
     object.
   - Preference is given to idioms defined as the main form of another
     idiom using the `Lemmaverweis` tag on the German-language Wiktionary.
   These factors do not always give the an unambiguous choice of a
   canonical form for a group and some judgment is involved.

   A number of listings are printed to stdout to aid in the review of the
   assignment:

   1. Cases where we have selected a canonical form which gives a count for
   the group <= the count of the the Wiktionary main form (from
   the `Lemmaverweis` tag).

   2. The count for the group defined using the Wiktionary main form may be
   ill-defined, as we might have defined a group where Wiktionary has
   assigned different main forms to the entries in our group. Therefore,
   we print a listing when this occurs.

   3. Print records and raise exception where:
      a) The `n_final` for an item in a group is at least five more than
      the `n_final` on the canonical form we selected for the group.
      b) The headword for the item is not in an OK-list that we checked to
      confirm that we are ok with exceeding the threshold of five. See the
      comments by the `OK_LIST` definition for the reasons we are okay with
      these assignments. (Some of the values in `OK_LIST` were identified
      by setting the `BUFFER` threshold to 1 instead of 5.)

  Larger (and less important) listings are saved to output files. These
  listings are for groups where there is one other idiom in the group not
  equal to the canonical form but with count >= the count for the canonical
  form. The listings are less important because many of the selected groups
  will have this occur because 'etwas' and 'jemande[mn]' aren't included
  when making the regular expressions that identify the counts, so for
  example, the idioms 'etwas im Kopf haben' and 'im Kopf haben' have the
  same counts by construction. For such groups, we pick the idiom that
  includes the direct object, which most of the time is an idiom from the
  German-language Wiktionary. The listing could be improved by excluding
  such groups.

Finally, note that the headwords in `COUNTS_FILE` can be a subset of the
headwords in `MANUAL_FILE`, although its expected that using this program
in such a manner would only be temporary. When this occurs, the `n_` output
variables will be empty for records not in `COUNTS_FILE`.

If `re1` or `re2` differ on `COUNTS_FILE` compared to `MANUAL_FILE`, then
the value from `COUNTS_FILE` will be used in the output file (because this
would be the value used to generate the counts. (The value could differ in
`MANUAL_FILE` if the regex was changed after saving the input to the
program that generates the counts.)
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
OUTPUT_FILE = 'output/redewendungen_final.txt'
BUFFER = 5

# Don't give warnings when these headwords have more hits than the main form.
OK_DICT = {
  'ein Herz fassen': ('Subset of words in main form, but main form '
                 'is more freq than just subset by large amount'),
  'etwas in Grenzen halten': "Same as 'ein Herz fassen'",
  'guter Hoffnung':          "Same as 'ein Herz fassen'",
  'Dorn im Auge':            "Same as 'ein Herz fassen'",
  'auf dem Spiel':           "Same as 'ein Herz fassen'",
  'jemandem in die Karten schauen': ("Same as 'ein Herz fassen', "
     "especially after adding more negations in addition to 'nicht'"),
  'alt werden': ('Will not be more frequent than main form after adding '
                 'manual review results.'),
  'macht nichts': "Same reason as 'alt werden'",
}

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
    #local_le_dewk[['headword_x', var,'mf_wk',
    #               'n_final','n_final_man','n_final_wk']].to_csv(
    #                 f'output/{var}_not_using.txt')

    listing3 = ((df.n_final != '') & (df.n_final_man != '')
         & (~df.headword_x.isin(OK_DICT))
         & (df.n_final.astype(float) >= BUFFER + df.n_final_man.astype(float)))
    if listing3.any():
        print(df[listing3][['headword_x','cf_man',
                            'dewk_main_form_on_variant','n_final',
                            'n_final_man']])
        print(f'{var}: Frequency of canonical form is less than '
                          f'frequency of group item by more than {BUFFER}')

    print_geq_cf(df, var)

LVW_DICT = {}
def add_to_lvw_dict(x):
    x_list = x.split(':')
    nebenform = x_list[0]
    mainform = x_list[1]
    if mainform:
        if nebenform in LVW_DICT:
            raise ValueError(f'{nebenform=} in dict twice')
        else:
            LVW_DICT[nebenform] = mainform

def read_lemmaverweis():
    lvw_df = pd.read_csv('Lemmaverweis/output_lvw.txt',
                   sep='\t', quoting=csv.QUOTE_NONE)
    lvw_df['lemma_main_form_1'] = lvw_df.lemma_main_form_1.fillna('')
    lvw_df['Value'] = lvw_df.headword + ':' + lvw_df.lemma_main_form_1
    lvw_df.Value.map(add_to_lvw_dict)

def check_both_languages(df_):
    en_df = pd.read_csv('other_lang/en/en.txt', sep='\t',
                        quoting=csv.QUOTE_NONE, names=['headword'])
    de_df = pd.read_csv('raw/v1.txt', sep='\t',
                        quoting=csv.QUOTE_NONE, names=['headword'])
    df_['in_de'] = np.where(df_.headword.isin(de_df.headword), 1, 0)
    df_['in_en'] = np.where(df_.headword.isin(en_df.headword), 1, 0)
    just_df = df_[df_.source == 'der'].copy()
    if (~de_df.headword.isin(just_df.headword)).any():
        print(de_df.headword[~de_df.headword.isin(just_df.headword)])
        raise ValueError('Idiom from German Wiktionary not in final file with '
                         "source 'der'")
    if (~en_df.headword.isin(df_.headword)).any():
        print(en_df.headword[~en_df.headword.isin(df_.headword)])
        raise ValueError('Idiom from English Wiktionary not in final file')

def make_links(headword, in_de, in_en):
    title = urllib.parse.quote(headword.strip().replace(' ','_'))
    de_title = f'https://de.wiktionary.org/wiki/{title}' if in_de else ''
    en_title = f'https://en.wiktionary.org/wiki/{title}' if in_en else ''
    return {'de': de_title, 'en': en_title}

def check_is_blank_or_headword(df, var):
    prob = ~((df[var] == '') | (df[var].isin(df.headword)))
    if prob.any():
        print(df[prob][['headword',var]])
        raise ValueError(f'{var} not blank or in `headword`')

def check_group_size_gt_1(df, var):
    '''Raise exception if group includes only one idiom.'''
    df_subset = df[df[var] != ''].copy()
    only1 = df_subset.groupby(var).size()[
                            df_subset.groupby(var).size() == 1]
    if len(only1):
        print(only1)
        raise ValueError(f'{var} defines group with only one idiom.'
                          ' See above listing.')

def print_geq_cf(df, var):
    '''Create broader listing than Listings 1-3 mentioned in docstring.

    For a given set of items grouped by canonical form (abbreviated 'cf',
    which = 'main_form' or 'related_headword'), print records where there
    is at least one other item in the group that is not the canonical form
    and has frequency >= that of the canonical form.

    In cases where the canonical form considered is 'main_form' and the
    group has the main form assigned to `dewk_main_form_on_variant`, do
    not print, since cases where freq > canonical form freq are already in
    Listings 1-3, and when freq = canonical form freq, we don't need to
    inspect.
    '''
    print_df = df[ (df.headword_x != df[var]) & (~df[var].isnull())
          & (df[var] != '')
          & (df.n_cum_1.astype(float) >= df.n_final_man.astype(float))]
    dewk_mainform_df = df[   (df.dewk_main_form_on_variant == df[var])
                           & (var == 'main_form')
                         ]
    df_mod = df[   df[var].isin(print_df[var])
                & ~df[var].isin(dewk_mainform_df[var])]
    print(f'\nSaved {len(df_mod)} records where count for at least 1 item'
          f' in group is >= count for the {var}')
    df_mod = df_mod.sort_values(var)
    df_mod[['headword_x','in_de','in_en','re1','re2','main_form',
            'related_headword','dewk_main_form_on_variant','n_cum_1','n_cum_2',
           ]
          ].to_csv(f"output/review_freq_geq_{var}.txt", sep='\t', index=False)

def check_comment_math(n_cum_1, n_manual, n_manual_cmt):
    '''Cross-check `n_manual_cmt` vs `n_cum_1` and `n_manual`.

    Often `n_manual` is determined by sampling or inspecting the full
    set of matches to `re1` (which are counted in `n_cum_1`). When this
    occurs, the results are reported in `n_manual_cmt` in a semi-structured
    manner. This function cross-checks the comment with the values as
    follows:

    If count of n found when checking all matches, `n_manual_cmt` should
    start with: 'n_manual/n_cum_1[;$]'. We check for a semi-colon or
    line-terminator here because sometimes we report a count that (for
    reasons explained in the rest of `n_manual_cmt`) are not used as the
    final count.

    If count of 0 found when checking sample (of size M), `n_manual_cmt`
    should start with: '0/M*n_cum_1 found, so assume 0.5 matches'.
    `n_manual` should then equal `round(0.5/M*n_cum_1)`. Currently only M
    in [50,100,200] is used so we check this too, but it would not be a big
    deal to add values or eliminate this check if we want larger samples.

    If non-zero count (call it `n_c`) is found with sample, `n_manual_cmt`
    should start with: '=n_c/M*n_cum_1', where `n_manual` is then the
    result of this calculation.

    Finally, we check that any `n_manual_cmt` that contains the phrase
    'assume 0.5' is formatted as described in the second paragraph.

    Raises
    ------
    ValueError if any check fails.
    '''

    re1 = re.compile(r'^(\d+)/(\d+)\*(\d+) found, so assume 0\.5 matches')
    result1 = re1.search(n_manual_cmt)
    re2 = re.compile(r'^(\d+)/(\d+)[;$]')
    result2 = re2.search(n_manual_cmt)
    re3 = re.compile(r'^=(\d+)/(\d+)\*(\d+)')
    result3 = re3.search(n_manual_cmt)
    re4 = re.compile(r'assume 0\.5')
    result4 = re4.search(n_manual_cmt)

    if result1:
        if int(n_cum_1) != int(result1.group(3)):
            print (f'{n_cum_1=}, but {n_manual_cmt=}')
            #raise ValueError(f'{n_cum_1=}, but {n_manual_cmt=}')
        if int(result1.group(2)) not in [50, 100, 200]:
            raise ValueError(f'Sample sizes should be 50, 100, or 200'
                             f' {n_manual_cmt=}')
        if int(result1.group(1)) != 0:
            raise ValueError(f'Expected 0 counts {n_manual_cmt=}')
        comment_implies = round(0.5 / int(result1.group(2))
                                * int(result1.group(3)))
        if int(n_manual) != comment_implies:
            raise ValueError(f'{n_manual=}, but {comment_implies=}'
                             f' from {n_manual_cmt=}')
    elif result2:
        if int(n_cum_1) != int(result2.group(2)):
            raise ValueError(f'{n_cum_1=}, but {n_manual_cmt=}')
        if int(n_manual) != int(result2.group(1)):
            raise ValueError(f'{n_manual=}, but {n_manual_cmt=}')
    elif result3:
        if int(n_cum_1) == 0:
            raise ValueError('If 0 matches found, then assume 0.5'
                             f' {n_manual_cmt=}')
        if int(n_cum_1) != int(result3.group(3)):
            raise ValueError(f'{n_cum_1=}, but {n_manual_cmt=}')
        if int(result3.group(2)) not in [50, 100, 200]:
            raise ValueError(f'Sample sizes should be 50, 100, or 200'
                             f' {n_manual_cmt=}')
        comment_implies = round(int(result3.group(1)) / int(result3.group(2))
                                * int(result3.group(3)))
        if int(n_manual) != comment_implies:
            raise ValueError(f'{n_manual=}, but {comment_implies=}'
                             f' from {n_manual_cmt=}')

    if result4 and not result1:
        raise ValueError(f'Incorrect format used for assuming 0.5 matches'
                         f' {n_manual_cmt=}')

def check_manual_comment(df):
    '''Raise exceptions if non-standard formatting is used in `n_manual_cmt`.
    '''
    def msg(series, msg):
        if series.any():
            print(df[series][['headword','n_manual_cmt']])
            raise ValueError(msg)

    # comments are phrases or sentences, but we do not capitalize the first
    # letter in the comment or put a period at the end
    prob1 = df.n_manual_cmt.str.contains('^[A-Z]')
    msg(prob1, '`n_manual_cmt` starts with a capital letter')
    prob2 = df.n_manual_cmt.str.endswith('.')
    msg(prob2, "`n_manual_cmt` ends with '.'")

    # use semi-colons, not colons to delimit multiple comments
    prob3 = df.n_manual_cmt.str.contains(':', regex=False)
    msg(prob3, "`n_manual_cmt` contains ':'")

    # Use the term 'match(es)' instead of 'hit(s)'
    prob4 = df.n_manual_cmt.str.contains('[Hh]it')
    msg(prob4, "`n_manual_cmt` contains 'hit' or 'Hit'")

    for row in df[['n_cum_1','n_manual','n_manual_cmt']].values:
        check_comment_math(row[0], row[1], row[2])

#------------------------------------------------------------------------------
# Main Entry Point
#------------------------------------------------------------------------------
counts_df = pd.read_csv(COUNTS_FILE, sep='\t', quoting=csv.QUOTE_NONE,
                usecols=['headword','orig order','re1','re2',
                         'n_cum_1','n_seq_1','n_ic_cum_1','n_ic_seq_1',
                         'n_cum_2','n_seq_2','n_ic_cum_2','n_ic_seq_2'],
                dtype=str, keep_default_na=False,na_values=[])

# users running this code after downloading from the repository will not have
# access to the 'output_lvw.txt' file but can just read in the
# dewk_main_form_on_variant form from the uploaded output file and merge
# it to counts_df. Users that want to create 'output_lvw.txt' themselves
# can follow the instructions in `run_wikwork.py`.

read_lemmaverweis()
counts_df['dewk_main_form_on_variant'] = counts_df.headword.map(
    lambda x: LVW_DICT.get(x, ''))

manual_df = pd.read_csv(MANUAL_FILE, sep='\t', quoting=csv.QUOTE_NONE,
                usecols=['headword','orig order','re1','re2',
                         'n_manual','n_manual_cmt',
                         'main_form','related_headword','source'],
                dtype=str, keep_default_na=False,na_values=[])
counts_df = counts_df.merge(manual_df, how='right', left_on='headword',
                right_on='headword', validate='1:1', suffixes=['_cnt',''])
counts_df['sort_order'] = counts_df['orig order']
counts_df['id'] = counts_df['orig order'].astype(int)
# This for-loop is to support the case where we might have gotten the
# counts only on a subset of idioms. However, we might still have the
# regular-expression groups in the file with the manual results, so we
# copy those to `re1` and `re2` here.
for revar in ['re1','re2']:
    counts_df[revar + '_cnt'] = counts_df[revar + '_cnt'].fillna('')
    counts_df[revar] = np.where(counts_df[revar + '_cnt'] != '',
                              counts_df[revar + '_cnt'], counts_df[revar])
check_both_languages(counts_df)
counts_df['n_final'] = np.where(counts_df.n_manual != '',
                                counts_df.n_manual, counts_df.n_cum_1)
links = [ make_links(row[0], row[1], row[2]) for
          row in counts_df[['headword','in_de','in_en']].values]
#counts_df['link'] = counts_df.headword.map(hw_to_title)
counts_df['link_de'] = [ row['de'] for row in links ]
counts_df['link_en'] = [ row['en'] for row in links ]
check_is_blank_or_headword(counts_df, var='main_form')
check_is_blank_or_headword(counts_df, var='related_headword')
check_group_size_gt_1(counts_df, var='main_form')
check_group_size_gt_1(counts_df, var='related_headword')
check_main_form_most_freq(counts_df, var='main_form')
check_main_form_most_freq(counts_df, var='related_headword')
check_manual_comment(counts_df)
outvars=['headword','id','sort_order','re1','re2',
         'n_cum_1','n_seq_1','n_ic_cum_1','n_ic_seq_1',
         'n_cum_2','n_seq_2','n_ic_cum_2','n_ic_seq_2',
         'n_manual','n_manual_cmt','n_final',
         'main_form','related_headword','dewk_main_form_on_variant',
         'link_de','link_en']
counts_df = counts_df.sort_values(['id'])
counts_df[outvars].to_csv(OUTPUT_FILE, sep='\t', quoting=csv.QUOTE_NONE,
                          index=False)

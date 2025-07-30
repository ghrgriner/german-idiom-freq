# Overview

This program reads a file of idioms (in German, _Redewendungen_)
and a text corpus and estimates the number of times each idiom appears in the corpus using
[regular expressions](https://en.wikipedia.org/wiki/Regular_expression).
The program is applied to all entries in the [Redewendung](https://de.wiktionary.org/wiki/Kategorie:Redewendung_(Deutsch))
category in the German-language (version of) Wiktionary, retrieved 18 June 2025 (n=2,692)
and all entries in the [German idioms](https://en.wiktionary.org/wiki/Category:German_idioms)
category in the English-language (version of) Wiktionary, retrieved 11 July 2025 (n=953), for
a total of n=3,224 distinct idioms.
We have also grouped related idioms and designated a
primary variant for each group. For idioms where the initial set of
regular expressions
returned a large number of matches in the corpus that we thought were not
likely to have the given idiomatic meaning, we manually reviewed the
set of matches or a random sample of the set and
created corrected estimates.

The data that is most likely of interest to repository visitors
is the output file [output/redewendungen\_final.txt](https://github.com/ghrgriner/german-idiom-freq/blob/main/output/redewendungen_final.txt)
and the
variables `headword`, `main_form`, `related_headword`, and
`n_final` (where this last variable is the frequency count,
possibly manually corrected). The `main_form` and `related_headword`
variables can be used to identify idiom variants and groupings, if desired.

The corpus used was a transformation of the
February 2022 extract of the German-language Wikipedia as created by
Philip May, available for download at:
    https://github.com/GermanT5/wikipedia2corpus

See [LICENSE.txt](LICENSE.txt) for complete license and attribution
details for the corpus, the German-language Wiktionary, the English-language Wiktionary, and
this repository.

The approximate frequencies may also be used by the repository author
to create an Anki flashcard deck with example excerpts from Wikipedia
or Wiktionary that is ordered by idiom approximate frequency. (TBD: add link
to deck if published.)

# Trademark Notice
Wiktionary and Wikipedia are
trademarks of the Wikimedia Foundation and are used with the permission of
the Wikimedia Foundation. We are not endorsed by or affiliated with the
Wikimedia Foundation.

# Methods

# Input Files

Three different input idiom files are used when running the programs.
These files all contain the same values in the `headword` variable, but
the other variables differ in each file. Multiple files are used so that
the sequence of programs can be run in one pass. Variables on later files
are created based on output from the preceding programs.

The input file of idioms used by [run\_wp2022\_counts.py](https://github.com/ghrgriner/german-idiom-freq/blob/main/src/run_wp2022_counts.py)
has four variables.
- **headword**: Headword on the Wiktionary page.
- **orig order**: Sort order
- **re1**: A group of regular expressions or placeholders (that will be
  replaced in `count_regexes.py` by regular expressions). See the docstring
  in `count_regexes.py` for complete details, but we have also given a
  high-level summary [below](#Regular Expression Groups in Input File).
- **re2**: An optional second group of regular expressions or placeholders.
  These counts were sometimes used in an exploratory manner, e.g., to
  determine which of two variant forms of an idiom were more common.

There is also an input text file of regular expression placeholders
([input/endehw\_verb\_forms.txt](https://github.com/ghrgriner/german-idiom-freq/blob/main/input/endehw_verb_forms.txt))
used by the same program. The input text file is converted to a dictionary with
key of `placeholder` and value of `replacement`, and the dictionary is
passed to `count_regexes`. The variables on this file are:
- **placeholder**: A string consisting of all capital letters or underscores.
This as used as a placeholder in `re1` and `re2` in the previous input file.
The placeholder is replaced with the regular expression in the `replacement`
variable when the program is run.
- **replacement**: See previous bullet.
- **comment**: An optional explanatory comment.

Once `run_wp2022_counts.py` is run, the counts can be manually corrected,
if desired. The programs
[save\_matches\_for\_sampling.py](https://github.com/ghrgriner/german-idiom-freq/blob/main/src/save_matches_for_sampling.py)
and
[sample\_for\_review.py](https://github.com/ghrgriner/german-idiom-freq/blob/main/src/sample_for_review.py)
can be used to generate samples or full listings of selected idioms for review.
The input file of idioms used by this program has the variables:
- **headword**: From first input file
- **n_manual_sampsize**: Either 'all' or the sample size to use to
  create file(s) with the starting set of matches and the sample.
  If empty, no files will be created for the given idiom.

The final file to run is [post\_process.py](https://github.com/ghrgriner/german-idiom-freq/blob/main/src/post_process.py).
This takes as input a third input file and merges these fields onto
the file or counts obtained from `run_wp2022_counts.py` to generate the
final output file. Various diagnostics are performed and listings created.
see the docstring of the file for details.

This third input file of idioms has the variables:
- **headword**: From first input file
- **n_manual**: [Manually corrected](#Manual-Correction-of-Frequency) count
- **n_manual_cmt**: Manual correction comment
- **main_form**: This assigns a canonical form to related idioms for
  the purpose of grouping related idioms. For example, 'Bauklötze stauen'
  and 'Bauklötzer stauen' both exist in the input file and we assign
  'Bauklötze stauen' as the canonical form.
  This is used for minor differences in the headwords, or differences
  with an obvious meaning ('gute Karten haben', 'schlechten Karten haben').
  Here 'canonical form' typically means the most frequent form observed,
  but when the difference is small we may use the canonical form given in
  the German-language Wiktionary (as identified in the `{{Lemmaverweis|}}` tag).
  See [Related Entries](#Related-Entries) below for details.
- **related_headword**: This selects one idiom as a group identifier for
  the purpose of grouping related idioms. It is like the `main_form` variable,
  except the idioms may be less closely related. Often the relationship is
  idioms with the same or similar meanings where the idioms are not
  minor variants of each other.
  See [Related Entries](#Related-Entries) below for details.
- **dewk_main_form_on_variant**: The main form for the page as assigned
  by the `Lemmaverweis` tag on the German-language Wiktionary for the page. See the
  `run_wikwork.py` program for instructions how the `Lemmaverweis.txt` input
  file can be obtained, but users who just want to use the same values
  we used can just take the column from the output file and merge it
  to the appropriate dataset in `post_process.py`. This variable has a longer
  name to emphasize that in the Wiktionary, the main form is only assigned
  on the wiki pages for the variant forms (sub-forms), so this will be blank
  for the main forms, unlike `main_form` which we always populate for the
  main forms too (as long as the group has more than one idiom).

This third input file used when running the program has not been uploaded,
but it can be recreated by selecting the first four columns listed above from
the program output (or the first five columns if using the same
`dewk_main_form_on_variant` variable is desired).

# Output File

- **headword**: From the first input file.
- **id**: From the `orig order` input field (so currently identical
  to `sort_order`, but if we decide to update the program to add idioms,
  the `sort_order` of existing records may change, while the `id` will
  stay the same.
- **sort_order**: From the `orig order` input field
- **re1**: From first input file
- **re2**: From first input file
- **verb_search_cat_1**: Is set to 'dict', 'stem', or ''. This is set to
  'dict' if the idiom contains a verb that was looked up in the 'VERB\_FORMS'
  dictionary in the program. This is 'stem' if an expression was found
  that meets the definition of 'probable verb stem' (see [the next section](#Regular-Expression-Groups-in-Input-File)
  for details). There is currently one idiom where the criteria for both
  'dict' and 'stem' are met, and the value is set to 'dict'. In all other
  cases, the value is ''. The intent of this field is to easily stratify
  the idioms by those that contain a verb and those that don't, for example,
  to inspect whether not capitalizing the first letter in regexes has
  a significant impact, a limitation that only applies to the verb regex.
- **n_cum_1**: Counts number of corpus entries for which all regular
  expressions in `re1` match. The match is case-sensitive.
- **n_seq_1**: Gives cumulative number of matches after applying each
  regular expression in `re1`, i.e. if `re1` is a group of three regexes,
  this will have the format 'n1 -> n2 -> n3' where n1 is the count for
  matching only the first regex, n2 is the count for matching the first and
  second regex, and n3 is the count for matching all three (and n3 = `n_cum_1`).
  Note that if `re1` contains a 'MANUAL\_REVIEW' placeholder, the count
  given in this field is the value from the previous step, and not the result
  of the manual review (which is in `n_manual`).
- **n_ic_cum_1**: Like `n_cum_1`, but case-insensitive.
- **n_ic_seq_1**: Like `n_seq_1`, but case-insensitive.
- **n_cum_2**, **n_seq_2**, **n_ic_cum_2**, **n_ic_seq_2**,
  **verb_search_cat_2**: Like the
  preceeding five variables, but based on `re2` instead of `re1`.
- **n_manual**: From second input file
- **n_manual_cmt**: From second input file
- **n_final**: `n_manual` if not empty, otherwise, `n_cum_1`.
- **main_form**, **related_form**, **dewk_main_form_on_variant**: See
  documentation [above](#Input-Files)
- **link_de**: Link to the (German-language) Wiktionary page for headwords
  in the 'Redewendungen' category on this Wiktionary.
  See the [Overview](#Overview) section for the retrieval date used
  when generating the list of headwords.
- **link_en**: Link to the (English-language) Wiktionary page for headwords
  in the 'German idioms' category on this Wiktionary.
  See the [Overview](#Overview) section for the retrieval date used
  when generating the list of headwords.

# Methodological Details

## Related Entries

It's useful to identify related idioms. For example, if multiple idioms have
the same meaning, then language learners will likely want to know which is
the more common variant, especially among variants that are otherwise the
same (i.e., no difference in register). When creating flashcards to learn
idioms, idioms in groups that have the same meaning may need to be handled
differently (e.g., if a user creates a task where the goal is to produce
one of the idioms, there will need to be sufficient context which is desired).

Consider the following group of idioms: {'einen Korb geben', 'jemandem einen Korb geben',
'einen Korb bekommen', 'einen Korb kriegen'}. The first two have
`main_form` set to 'jemandem einen Korb geben' and the last
two have `main form` set to 'einen Korb bekommen'. All four have
`related_headword` set to 'jemandem einen Korb geben', as the variants
with 'geben' have higher frequency than those with 'bekommen' or 'kriegen'.

The docstring of `post_process.py` lists some of the factors we consider
when assinging which variant as the 'canonical form' for a group.

Often `related_headword` is used to define groups of synonyms, but it
might also be used to group variants of the same concept, e.g.
'jemandem Rede und Antwort stehen' vs 'jemanden zur Rede stellen', or
'seine Schäfchen im Trockenen haben' vs 'seine Schäfchen ins Trockene bringen'.

The distinction between `main_form` and `related_headword` is similar to the
distinction in the German-language Wiktionary between the lemma cross-references created using
the `Lemmaverweis` template and other groupings (especially 'Synonyme' and 'Sinnverwandte Wörter' (synonyms)).
When assigning the canonical forms for this project, we perform some cross-checks
against the Wikipedia main form assigned (see `post_process.py` docstring), but do
not always use the same assignment as Wiktionary.
Furthermore, no programmatic cross-checks were done to compare our `related_headword`
field with the synonyms in Wiktionary.

Note that even if Wiktionary does not imply a preferred form, we do not intend
to constantly revise the `main_form` and `related_headword` fields if there
are minor changes in counts due to changes in the program logic, input regular
expressions, addition of more corpora, etc.

## Regular Expression Groups in Input File
The `re1` and `re2` fields are what we will call a regular expression group
in the input file. This is a sequence of regular expressions (regexes) or
placeholders delimited by '+'. We count the idiom as appearing in a corpus
sentence if all regexes match. The input file can contain placeholders. We
adopt the convention that placeholders are denoted by all capital letters
or underscores. In most cases, placeholders must occupy the whole place in
the sequence (i.e., in the sequence that is delimited by '+'), but in a
few cases they may be contained in a larger regex. Placeholders are replaced
in the program by a more complex regular expression. For example, the idiom
'zu Potte kommen', the regular expression group is 'zu Potte + KOMMEN'
and the second item in the group is 'KOMMEN', a placeholder that is replaced
in the `count_regexes.py` program by '\bkomm|\bk[aä]m|\bgekommen'. Placeholders
that start with '\_' are similar, except they do not have the '\b'
word-boundary anchor at the start of each string and therefore are used when
the verb in the idiom has a separable prefix.

There are a number of placeholders that start with 'SEIN' that can be used
within larger regular expressions. For example, 'SEINMA' is replaced in the
program by: `r'(\b[MDSmds]einen\b|\b[Ii]hren\b|\b[Uu]nseren\b)'` to match
the most common masculine accusative forms of 'sein', 'mein', 'dein', 'ihr',
'Ihr', and 'unser'.

If the regular expression / placeholder is 'es' or a preposition, it may
be expanded to include common contractions. For example 'zu' is expanded
to include 'zum' and 'zur'. See the program for complete details.

'SICHD', 'SICHA', and 'SICHB' are placeholders that match dative, accusative,
and both dative and accusative reflexive pronouns respectively.
The final regex is modified so that if the first character is a lower-case
letter, an upper-case letter is also permitted, and the regex is surrounded
by word-boundary markers.
Note that we use the convention that 'SICHD','SICHA','SICHB' are used for
objects of prepositions and 'SICH' (which also matches dative and
accusative reflexive pronouns) is used for objects of verbs.

Any regex not processed by the above steps is modified if it is
not a probable verb stem. The modification is: if the first character is
a lower-case letter, an upper-case letter is also permitted, and the regex
is surrounded by word-boundary markers. A probable verb stem: (1) does not
contain a space or start with a capital letter, (2) must occur in a group
with more than one regex, not counting a placeholder that says 'MANUAL\_REVIEW',
(3) is in the final position in the group, excluding 'MANUAL\_REVIEW' or
'SICH' entries, (4) is not contained in the `NOT_VERB_STEM` list.

An input regex of 'MANUAL\_REVIEW' is ignored by this module. This can be
used to denote the suspicion that the input regexes may be too sensitive
(i.e., cast too wide a net) and the output counts may need to be corrected.

Finally, 'ß' is replaced in the regex from any of the above steps by
'(ß|ss)'.

## Conventions in Input File
In addition to the placeholders in the above section, the following conventions
are used in the input file.
- 'MANUAL\_REVIEW' is the last item in any group where it is included.
- 'SICH' is the last item in the group or the second to last if the group also
  contains 'MANUAL\_REVIEW'.
- Phrases that consist of some combination of a definite article, adjective and
  noun (but no verb) use a regular expression in the input file to match not
  only the nominative form of the expression (as in the `headword` field), but
  also accusative, dative, and genitive cases. The nominative form is then usually
  put in `re2` for comparsion. Phrases using this format have `re1` starting with
  '\b[Dd]'.
- Phrases that consist of some combination of an indefinite article or 'kein-',
  adjective,  and noun (but no verb) often have a regular expression in `re1`
  of the form `ein\w* Nomen` with the idiom as in the `headword` field in `re2`.
  However, sometimes `re2` is the idiom without any article at all (to show that
  the matches in such a case are zero). In this case, we have not written a complex
  expression in `re1` and just leave the value as is found in `headword`.
- For the above two bullets, if a verb is present in the idiom, we only use the
  grammatical case of the phrase that was used in the idiom, even we have not
  included the verb in the regular expression.
- <a name="separable-verb-conventions"></a>
  Idioms containing separable verbs are usually formatted as in this example for
  'ein Fass aufmachen', where the regex group is then:
  'ein Fass + \bauf(\b|m|z|g) + \_MACHEN'. The second item in the list is a
  regex for just the verb prefix (without capitalization), followed by either:
  the end of the word, the first letter of the verb stem,
  'z' (in order to match 'aufzumachen') or 'g' (in order to match 'aufgemachen').
  The '\_MACHEN' placeholder is replaced by 'mach|gemacht'. Note the absense of word
  boundary markers in this last
  regex because the prefix might occur in front of the stem. In the case where
  we feel this might lead to too many matches, then the separable verb would
  just be entered as 'AUFMACHEN' placeholder and the necessary forms listed in
  the `VERB_FORMS` dictionary in `count_regexes.py`. For example, this is done for
  the verb 'zutun'.

## Manual Correction of Frequency

In cases where the regular expressions incorrectly identify a large number
of sentences that do not fit the idiom definition, the counts are corrected,
usually by sampling 50, 100, or 200 records, and the number of matches found is
multiplied by the proportion of matches in the sample. If 0 matches are found in the sample
then we assume 0.5 matches were found. The purpose of this is to reduce the
estimated frequency of idioms where the regular expressions identify far
too many false positives so that we do not report among the most-frequent
idioms some idioms that are obviously overcounted. It is possible that the
estimate derived from assuming 0.5 matches is still overcounting but we do not
consider it to be worth the effort to improve the estimate further, given
there are a variety of other factors that also lead to noise in the frequency
estimate that are discussed [in the following section](#Discussion). Users
that are concerned about any overestimation from the assumption of 0.5 matches in the
sample can inspect the `n_manual_cmt` field for the small number of records
where it is populated and exclude the desired records.

Manual review has also been done for some records with a lower frequency.

Note that records may have a 'MANUAL\_REVIEW' placeholder in the `re1`
or `re2` field even in cases manual review has not been conducted or reported.
Furthermore, the absence of a 'MANUAL\_REVIEW' tag in the `re1` or `re2` field
does *not* indicate that we have reviewed the results and are satisfied that the
majority of the matches represent the desired idiomatic usage.

# Results
The output file is linked [above](#Overview). Brief summary results
and a listing of the top-50 idioms by frequency are
[available on the repository wiki](https://github.com/ghrgriner/german-idiom-freq/wiki/Results).

# Discussion

The strengths of the approach we have used are its relative simplicity
(compared to writing a proper parser), transparency (except for the random
sampling in the manual correction), flexibility (where large errors can
be corrected by manually checking the corpus for matches), and use of
materials with licenses no more restrictive than CC-BY SA 4.0.

There are a number of caveats / limitations to identifying the presence of
an idiom in a sentence using the method we have used.

1. We might miss the presence of an idiom in a sentence if additional words
are inserted. For example, if we have a regular expression to match a
prepositional phrase consisting of (prep. + article/determiner + noun), an
adjective might be inserted between the article/determiner and noun in the
corpus text. Attempts to counter this by breaking the regular expressions
into smaller pieces increase the likelihood of false-positive matches.

2. The idiom might commonly appear with some words exchanged for other
words, e.g. definite articles swapped for determiners. Sometimes such
variants are their own entries in the idiom list, but sometimes not.

3. We generally ignore distinctions between 'jemandem', 'jemanden', and
'etwas' in the idioms when creating the regular expressions, as there
appears to be no easy way to incorporate the information. For example, for
the idiom 'ein Auge auf etwas werfen', the regular expression group input
into the program is 'ein Auge auf + WERFEN'.

4. The regular expressions used to match verb forms generally only match
the prefix of the present, past, and conditional stems, instead of the
various individual conjugated forms. For this reason, the pattern being
matched is not capitalized (out of concern it might match noun forms),
which means that matches will not be identified when the sentence starts
with the verb form.

5. We do not attempt to handle all possible contractions. If the regex from
the input file consists only of a preposition, then we do expand the regex
to allow for common contractions, 'zum', 'zur', 'aufm', "auf'm", etc...
Similarly, "`s" is included in the search when the regex in the input file
is 'es'. However, if the regex in the input file is a longer phrase that
contains a preposition, there is no additional search for contractions.
For example, if the regex in the input file is 'aus dem Ruder' (from the
idiom 'aus dem Ruder laufen'), the program does not expand the regex to
include "aus'm Ruder".

6. Even if the regular expression correctly identifies the phrase in the
sentence, the phrase might have a literal, rather than idiomatic, meaning.
For example, the German-language Wiktionary has the idiom 'mit jemandem gehen' with the
definition 'eine Liebesbeziehung mit jemandem führen' (go steady)[[1]](#ref-dewk-mit-jdm-gehen),
but the majority of the large number of matches returned by the regular
expressions used to find this idiom simply return sentences of people going
places with objects or going somewhere with a person people in a
non-romantic sense.

7. Important idioms might be missing from Wiktionary or not included in the
'Redewendung' or 'German idioms' category.

8. In some cases the input regular expression group indicates manual review should
be considered, but the manual review has not been performed.

9. The corpus might not match the [register](https://en.wikipedia.org/wiki/Register_(sociolinguistics))/formality that a user
wants to speak in, since it is derived from an online encyclopedia.

10. The repository author is not a native speaker of German.

11. The English Wiktionary includes some single-word separable-verb entries.
    This is contrast to the German-language Wiktionary where single-word
    entries were limited to nouns. Since these single-word separable-word
    entries have no objects or other words to further restrict the results,
    there is a risk that the regular expressions used will overestimate
    the frequency by a non-trivial amount. We may change
    [how separable verbs are handled](#separable-verb-conventions)
    to ensure that the string matching the separable prefix is either
    attached to the verb or detached after the verb. (See handling of the
    `ZU_GEHEN` placeholder in `count_regexes.py` for how this will likely be
    done.)

Despite these limitations, we feel the approach is adequate for our goal
of obtaining approximate frequency estimates for each idiom. We feel it's
most important for users to be able to identify the most common idioms to
prioritize those for study and we feel this approach is better, than say,
one possible alternative of simply studying 3000 idioms and giving equal
weight to all of them. There may of course be other ways for users to
obtain frequency estimates, including using the various so-called 'AI'
large language models. We are unsure of the accuracy of those methods
compared to our approach.

# Run-Time
The file `run_wp2022_counts.py` (which calls `count_regexes()` in `count_regexes.py`)
takes about 2 hours on our laptop to run. The optional `run_wikwork.py`
program probably takes an equivalent time to run, but we did not record the run-time.

# References
<a name="ref-dewk-mit-jdm-gehen">[1]</a>
Wiktionary-Bearbeiter, "mit jemandem gehen," Wiktionary, https://de.wiktionary.org/w/index.php?title=mit_jemandem_gehen&oldid=9446291 (abgerufen am 6. Juli 2025).

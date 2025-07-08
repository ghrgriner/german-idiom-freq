# Overview

This program reads a file of idioms (in German, _Redewendungen_)
and file(s) of corpora and estimates
the number of times each idiom appears in the corpora using
[regular expressions](https://en.wikipedia.org/wiki/Regular_expression).
The program is applied to all entries in the [Redewendung](https://de.wiktionary.org/wiki/Kategorie:Redewendung_(Deutsch))
category in German Wiktionary, retrieved 18 June 2025 (n=2,692).
This allows interested users to get a sense of the approximate frequency
of German idioms. We have also grouped related idioms and designated a
primary variant for each group. For idioms where the initial match
returned a large number of hits in the corpora that we thought were not
likely to have the given idiomatic meaning, we manually reviewed the
set of matches or a random sample of the set and
created corrected estimates.

The data that is most likely of interest to repository visitors
is the output file [output/dewki\_redewendung\_final.txt](https://github.com/ghrgriner/german-idiom-freq/blob/main/output/dewk_redewendungen_final.txt)
and the
variables `Redwendung`, `Hauptform`, and `n_final` (the frequency,
possibly manually corrected). The `Hauptform` variable can be used
to identify idiom variants, if desired.

The corpora used in this program are four German corpora from
Universität Leipzig[[1]](#ref-Leipzig) available for download at
https://wortschatz.uni-leipzig.de/en/download/German.

See [LICENSE.txt](LICENSE.txt) for complete license and attribution
details for the corpora, German Wiktionary, and this repository.

The approximate frequencies are also used by the repository author
to create an Anki flashcard deck with example excerpts from Wikipedia
or Wiktionary that is ordered by idiom approximate frequency. (TBD: add link
to deck when published).

# Input

The input file used by [get\_counts.py](https://github.com/ghrgriner/german-idiom-freq/blob/main/get_counts.py)
has four variables.
- **Redewendung**: Headword on the Wiktionary page.
- **orig order**: Sort order
- **re1**: A group of regular expressions or placeholders (that will be
  replaced in `get_counts.py` by regular expressions). See the docstring
  in `get_counts.py` for complete details, but we have also given a
  high-level summary [below](#Regular Expression Groups in Input File).
- **re2**: An optional second group of regular expressions or placeholders.
  These counts were sometimes used in an exploratory manner, e.g., to
  determine which of two variant forms of an idiom were more common.

Once `get_counts.py` is run, the counts can be manually corrected
if desired. A second input file can be passed to `post_process.py` along
with the output from `get_counts.py`. This second input file has the
columns:
- **Redewendung**: Same as above
- **n_manual**: [Manually corrected](#Manual-Correction-of-Frequency) count
- **n_manual_cmt**: Manual correction comment
- **Hauptform**: This gives the primary form for related idioms, e.g.,
  'Bauklötze stauen' and 'Bauklötzer stauen' both exist in the input
  file and have 'Bauklötze stauen' assigned as the primary form.
  This is also used to group idioms with the same meaning and structure
  but that use different words, e.g. 'aufpassen wie ein Haftelmacher',
  'aufpassen wie ein Heftelmacher', 'aufpassen wie ein Luchs',
  'aufpassen wie ein Schießhund'. We prefer to choose the most frequent
  form as the 'Hauptform'. Sometimes we have used [Google Books Ngram Viewer](https://books.google.com/ngrams/info)
  to compare frequency variants when corpus frequencies were low and/or zero.
  Note that we do not intend to
  constantly revise the Hauptform if there are minor swings in counts due
  to changes in the program, addition of more corpora, etc.

This second input file used when running the program has not been uploaded,
but it can be recreated by selecting the three columns listed above from
the program output.

# Output

- **Redewengung**: From input
- **sort_order**: From the `orig order` input field
- **id**: Also from the `orig order` input field (so currently identical
  to `sort_order`, but if we decide to update the program to add idioms,
  the `sort_order` of existing records may change, while the `id` will
  stay the same.
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
- **link**: Link to the Wiktionary page for the idiom

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
in the `get_counts.py` program by '\bkomm|\bk[aä]m|\bgekommen'. Placeholders
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
  only the nominative form of the expression (as in the `Redewendung` field), but
  also accusative, dative, and genitive cases. The nominative form is then usually
  put in `re2` for comparsion. Phrases using this format have `re1` starting with
  '\b[Dd]'.
- Phrases that consist of some combination of an indefinite article or 'kein-',
  adjective,  and noun (but no verb) often have a regular expression in `re1`
  of the form `ein\w* Nomen` with the idiom as in the `Redewendung` field in `re2`.
  However, sometimes `re2` is the idiom without any article at all (to show that
  the hits in such a case are zero). In this case, we have not written a complex
  expression in `re1` and just leave the value as is found in `Redewendung`.
- For the above two bullets, if a verb is present in the idiom, we only use the
  grammatical case of the phrase that was used in the idiom, even we have not
  included the verb in the regular expression.
- Idioms containing sepable verbs are usually formatted as in this example for
  'ein Fass aufmachen', where the regex group is then:
  'ein Fass + \bauf(\b|m|z|g) + \_MACHEN'. We see that there is a regex for
  just the prefix, than cannot be capitalized and is followed by a word-boundary
  marker, the first letter of the stem, 'z' (in order to match 'aufzumachen')
  or 'g' (in order to match 'aufgemachen'). The '\_MACHEN' placeholder is
  replaced by 'mach|gemacht'. Note the absense of word boundary markers in this
  regex because the prefix might occur in front of the stem. In the case where
  we feel this might lead to too many matches, then the separable verb would
  just be entered as 'AUFMACHEN' placeholder and the necessary forms listed in
  the `VERB_FORMS` dictionary in `get_counts.py`. For example, this is done for
  the verb 'zutun'.

## Manual Correction of Frequency

In cases where the regular expressions incorrectly identify a large number
of sentences that do not fit the idiom definition, the counts are corrected,
usually by sampling 50 or 100 records, and the hits found is multiplied
by the proportion of matches in the sample. If 0 hits are found in the sample
then we assume 0.5 hits were found. The purpose of this is to reduce the
estimated frequency of idioms where the regular expressions identify far
too many false positives so that we do not report among the most-frequent
idioms some idioms that are obviously overcounted. It is possible that the
estimate derived from assuming 0.5 hits is still overcounting but we do not
consider it to be worth the effort to improve the estimate further, given
there are a variety of other factors that also lead to noise in the frequency
estimate that are discussed [in the following section](#Discussion). Users
that are concerned about any overestimation from the assumption of 0.5 hits in the
sample can inspect the `n_manual_cmt` field for the small number of records
where it is populated and exclude the desired records.

Manual review has also been done for some records with a lower frequency. Often
these reviews were triggered when we discovered that few or no usable example
sentences were available in Wikipedia for the idiom while making a flashcard
deck for study. Usually in these cases it is simple to review all matches so
no sampling is done.

Note that records may have a 'MANUAL\_REVIEW' placeholder in the `re1`
or `re2` field even in cases manual review has not been conducted or reported.
Furthermore, the absence of a 'MANUAL\_REVIEW' tag in the `re1` or `re2` field
does *not* indicate that we have reviewed the results and are satisfied that the
majority of the hits represent the desired idiomatic usage.

# Results
The output file is linked [above](#Overview). We will likely at some point
add summary tables and/or listings to the repository wiki (TBD).

# Discussion

The strengths of the approach we have used are its relative simplicity
(compared to writing a proper parser), transparency (except for the random
sampling in the manual correction), flexibility (where large errors can
be corrected by manually checking the corpora for matches), and use of
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

7. Even if the regular expression correctly identifies the phrase in the
sentence, the phrase might have a literal, rather than idiomatic, meaning.
For example, German Wiktionary has the idiom 'mit jemandem gehen' with the
definition 'eine Liebesbeziehung mit jemandem führen' (go steady)[[2]](#ref-dewk-mit-jdm-gehen),
but the majority of the large number of hits returned by the regular
expressions used to find this idiom simply return sentences of people going
places with objects or going somewhere with a person people in a
non-romantic sense.

8. Important idioms might be missing from Wiktionary or not included in the
'Redewendung' category.

9. Although the corpora titles indicate 1 million records per corpus, the
Mixed-typical 2011 corpus and News 2015 corpus have less than a million
records each. We are not sure of the reason. The total number of records
in the 4 corpora used is 3.96 million.

10. In some cases the input regular expression indicates manual review is
likely required, but the manual review has not been performed in all cases.

11. The corpora used might not match the [register](https://en.wikipedia.org/wiki/Register_(sociolinguistics))/formality that a user
wants to speak in. Two of the corpuses used are news corpuses and a third is
Wikipedia. The fourth is a mixed-source corpus. (See [LICENSE.txt](./LICENSE.txt)
for details.) The point here is that idioms used in informal speech will
appear less in the news corpora than if corpora using more informal language
were searched.

12. The Mixed-source corpus that was used can contain text that is also in
the corpus sourced from Wikipedia.

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
The file takes about 5 hours on our laptop to run.

# References
<a name="ref-Leipzig">[1]</a>
D. Goldhahn, T. Eckart & U. Quasthoff: Building Large Monolingual Dictionaries
at the Leipzig Corpora Collection: From 100 to 200 Languages. In: Proceedings of
the 8th International Language Resources and Evaluation (LREC'12), 2012.

<a name="ref-dewk-mit-jdm-gehen">[2]</a>
Wiktionary-Bearbeiter, "mit jemandem gehen," Wiktionary, https://de.wiktionary.org/w/index.php?title=mit_jemandem_gehen&oldid=9446291 (abgerufen am 6. Juli 2025).

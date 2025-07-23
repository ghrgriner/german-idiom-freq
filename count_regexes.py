#    Count regular expresion combinations in corpora.
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

'''Count regular expression combinations in corpora.

This program takes a data frame of idioms and file(s) of a corpus or copora
and estimates the number of times each idiom appears in the corpora using
regular expressions.

Each idiom has should have one or two groups of regular expressions
(regexes) assigned in the input data frame (in the `re1` and `re2`
variables). Each group of regexes is a list of individual regexes delimited
by '+', e.g. '[regex1] + [regex2] + [regex3]'.  This module counts the
number of sentences in the corpora that match all of the regexes (`n_cum`).
The sequential counts are also given in a string (`n_seq`), i.e., the
number of matches for regex1, then the number of matches for regexes 1 and
2, then the number of matches for all three regexes as 'n1 -> n2 -> n3'.
The previously mentioned counts are done both in a case-sensitive
(`n_cum`,`n_seq`) and case-insensitive (`n_ic_cum`, `n_ic_seq`) manner.

Regexes that consist only of 'es' or a preposition are first expanded
to allow for endings that contract with a following definite article.
For example, the regex 'hinter' becomes 'hinter('?[ms])?', which matches
{"hinter","hinter'm", "hinterm", "hinter's", "hinters"}.
Then the regex is modified so that the first letter can be either capital
or lower case. Another matching option of '[Dd]a?r' + `orig_val` is added
if the preposition starts with a vowel and '[Dd]a' + `orig_val` if it
starts with a consonant. (Here `orig_val` is the original value of the
regex before any of the above modifications.) Word-boundary markers are
added to both sides of the two regexes and the two regexes are then joined
with '|'. If the input value was: 'es', then '`s\b|\b[Ee]s\b' is returned.

If a token in the regex group is in all-capital letters, this represents
a placeholder that is replaced in this module by the actual regex to use.
This is mostly for strong verbs but is also done for some common weak verbs
and 'SICH'. Usually the matching is done on the stem of the verb forms and
full Partizip. 2 form, but for some common verb forms (e.g.,
SEIN, HABEN, TUN, most forms are enumerated with word-boundaries at the
end of the pattern to prevent false-positive matches. Word-boundaries
are included at the start of the stem, unless the regex in the input file
started with '_', which was used to indicate a verb with a separable
prefix.

After this is done, any remaining occurence of an all-cap string that
starts with 'SEIN' in a regex is replaced with a regex giving the
appropriate variants of 'sein','mein','ihr', or 'unser'. For example,
'SEINMA' (where the trailing 'MA' stands for 'masculine accusative') is
replaced with variants of 'meinen', 'seinen', 'ihren', and 'unseren'.
The final regex is modified so that if the first character is a lower-case
letter, an upper-case letter is also permitted, and the regex is surrounded
by word-boundary markers.

Any regex not processed by the above steps is also modified as in the last
step of the previous paragraph as long as it is not a probable verb stem.
That is, if the first character is a lower-case letter, an upper-case
letter is also permitted, and the regex is surrounded by word-boundary
markers. A probable verb stem: (1) does not contain a space or start with
a capital letter, (2) must occur in a group with more than one regex,
excluding a regex that says 'MANUAL_REVIEW', (3) is in the final position
in the group, excluding 'SICH' or 'MANUAL_REVIEW' entries (4) is not
contained in the `NOT_VERB_STEM` list.

Finally, 'ß' is replaced in the regex in any of the above steps by
'(ß|ss)'.

An input regex of 'MANUAL_REVIEW' is ignored by this module. This can be
used as an indicator that the input regexes may be too sensitive
(i.e., cast too wide a net) and the output counts may need to be corrected.
'''

#------------------------------------------------------------------------------
# File: count_regexes.py
#------------------------------------------------------------------------------

__all__ = ['count_regexes']

import csv
from dataclasses import dataclass, field
from functools import partial
import multiprocessing
import os
import re
import warnings

import pandas as pd

#------------------------------------------------------------------------------
# Parameters
#------------------------------------------------------------------------------
warnings.filterwarnings('ignore', 'This pattern has match groups')

#------------------------------------------------------------------------------
# Constants
#------------------------------------------------------------------------------
# Input regexes that meet other criteria as 'probable verb stems' but are not
# treated as such. Recall that 'probable verb stems' do not have their first
# letter optionally capitalized or word boundaries added to the regex.
NOT_VERB_FRAGMENTS = ['bei','bekannt','gebunden','geduldig','gefressen',
                      'gestochen','gut','herum','keiner','los','mitgefangen',
                      'mitgehangen','sicher','weg','zusammen']
# entries that start with underscore are for use in separable verbs, so they
# do not have the '\b' to mark the start of the word boundary for the present
# and past forms.
VERB_FORMS = {'LIEGEN': r'\blieg|\bl[aä]g|\bgelegen',
              # Most of the time the key is just the infinitive, perhaps
              # preceded by an '_', but for 'hängen', we need to distinguish
              # between the strong and weak variants
              'HÄNGENHING': r'\bhäng|\bhing|\bgehangen',
              'HÄNGENHÄNGTE': r'\bhäng|\bgehängt',
              'STEHEN':  r'\bsteh|\bst[auüä]nd|\bgestanden',
              '_STEHEN': r'steh|st[auüä]nd|gestanden',
              'VERSTEHEN': r'\bversteh|\bverst[auüä]nd|\bverstanden',
              'GEHEN': r'\bgeh|\bging|\bgegangen',
              '_GEHEN': r'geh|ging|gegangen',
              'ZU_GEHEN': (r'\bgeh.*\bzu\b|\bging.*\bzu\b|'
                          r'\bzugeh|\bzuging|\bzuzugehen|\bzugegangen'),
              'HER_GEHEN': (r'\bgeh.*\bher\b|\bging.*\bher\b|'
                          r'\bhergeh|\bherging|\bherzugehen|\bhergegangen'),
              'ZERGEHEN': r'\bzergeh|\bzerging|\bzergangen',
              'VERGEHEN': r'\bvergeh|\bverging|\bvergangen',
              'LESEN' : r'\blese|\blies|\bl[aä]ß|\bgelesen',
              '_LESEN': r'lese|lies|l[aä]ß|gelesen',
              'SEHEN' : r'\bsehe|\bsieh|\bs[aä]h|\bgesehen',
              '_SEHEN': r'sehe|sieh|s[aä]h|gesehen',
              'EMPFEHLEN': r'\bempf[aeäö]hl|\bempfiel|\bempfohlen',
              'STEHLEN': r'\bst[aeäö]hl|\bstiel|gestohlen',
              'NEHMEN' : r'\bnimm|\bn[aäe]hm|\bgenommen',
              '_NEHMEN': r'nimm|n[aäe]hm|genommen',
              'ZIEHEN': r'\bzieh|\bz[oö]g|\bgezogen',
              '_ZIEHEN': r'zieh|z[oö]g|gezogen',
              'VERZIEHEN': r'\bverzieh|\bverz[oö]g|\bverzogen',
              'FLIEGEN': r'\bflieg|\bfl[oö]g|\bgeflogen',
              'SCHIEBEN': r'\bschieb|\bsch[oö]b|\bgeschoben',
              '_SCHIEBEN': r'schieb|sch[oö]b|geschoben',
              'SCHIEẞEN': r'\bschieß|\bsch[oö]ss|\bgeschossen',
              '_SCHIEẞEN': r'schieß|sch[oö]ss|geschossen',
              'SCHLIEẞEN': r'\bschließ|\bschl[oö]ss|\bgeschlossen',
              '_SCHLIEẞEN': r'schließ|schl[oö]ss|geschlossen',
              'GENIEẞEN': r'\bgenieß|\bgen[oö]ss|\bgenossen',
              'STOẞEN': r'\bst[oö]ß|\bstieß|\bgestoßen',
              '_STOẞEN': r'st[oö]ß|stieß|gestoßen',
              'KOMMEN': r'\bkomm|\bk[aä]m|\bgekommen',
              '_KOMMEN': r'komm|k[aä]m|gekommen',
              'BIETEN': r'\bbiet|\bbeut|\bb[oö]t|\bgeboten',
              '_BIETEN': r'biet|beut|b[oö]t|geboten',
              'VERBIETEN': r'\bverbiet|\bverbeut|\bverb[oö]t|\bverboten',
              'RIECHEN': r'\briech|\br[oö]ch|\bgerochen',
              'KRIECHEN': r'\bkriech|\bkr[oö]ch|\bgekrochen',
              'SCHLAFEN': r'\bschl[aä]f|\bschlief|\bgeschlafen',
              'HALTEN': r'\bh[aä]lt|\bhielt|\bgehalten',
              '_HALTEN': r'h[aä]lt|hielt|gehalten',
              'FALLEN': r'\bf[aä]ll|\bfiel|\bgefallen',
              'LASSEN': r'\bl[aä]ss|\bließ|\bgelassen',
              '_LASSEN': r'l[aä]ss|ließ|gelassen',
              'BLASEN': r'\bbl[aä]s|\bblies|\bgeblasen',
              'SAUGEN': r'\bsaug|\bsög|\bgesaugt|\bgesogen',
              '_SAUGEN': r'saug|sög|gesaugt|gesogen',
              'LAUFEN': r'\bl[aä]uf|\blief|\bgelaufen',
              '_LAUFEN': r'l[aä]uf|lief|gelaufen',
              'VERLAUFEN': r'\bverl[aä]uf|\bverlief|\bverlaufen',
              'VERLIEREN': r'\bverlier|\bverl[oö]r|\bverloren',
              '_FRIEREN': r'frier|\bfr[oö]r|gefroren',
              'BRINGEN': r'\bbring|\bbr[aä]chte|\bgebracht',
              '_BRINGEN': r'bring|br[aä]chte|gebracht',
              'DENKEN': r'\bdenk|\bd[aä]chte|\bgedacht',
              'SCHREIBEN': r'\bschreib|\bschrieb|\bgeschrieben',
              'SCHWEIGEN': r'\bschweig|\bschwieg|\bgeschwiegen',
              'BLEIBEN': r'\bbleib|\bblieb|\bgeblieben',
              '_BLEIBEN': r'bleib|blieb|geblieben',
              'TREIBEN': r'\btreib|\btrieb|\bgetrieben',
              '_TREIBEN': r'treib|trieb|getrieben',
              'REIBEN': r'\breib|\brieb|\bgerieben',
              'STEIGEN': r'\bsteig|\bstieg|\bgestiegen',
              '_STEIGEN': r'steig|stieg|gestiegen',
              'SCHEIDEN': r'\bscheid|\bschied|\bgeschieden',
              'WEISEN': r'\bweis|\bwies|\bgewiesen',
              'VERWEISEN': r'\bverweis|\bverwies|\bverwiesen',
              '_SCHIEßEN': r'schieß|sch[oö]ss|geschossen',
              'PFEIFEN': r'\bpfeif|\bpfiff|\bgepfiffen',
              'GREIFEN': r'\bgreif|\bgriff|\bgegriffen',
              'SCHREITEN': r'\bschreit|\bschritt|\bgeschritten',
              '_SCHREITEN': r'schreit|schritt|geschritten',
              'REITEN': r'\breit|\britt|\bgeritten',
              'SCHNEIDEN': r'\bschneid|\bschnitt|\bgeschnitten',
              '_SCHNEIDEN': r'schneid|schnitt|geschnitten',
              'SCHMEIẞEN' : r'\bschmeiß|\bschmiss|\bgeschmissen',
              '_SCHMEIẞEN' : r'schmeiß|schmiss|geschmissen',
              'SCHEIẞEN' : r'\bscheiß|\bschiss|\bgeschissen',
              'REIẞEN': r'\breiß|\briss|\bgerissen',
              '_REIẞEN': r'reiß|riss|gerissen',
              'BEIẞEN': r'\bbeiß|\bbiss|\bgebissen',
              '_BEIẞEN': r'beiß|biss|gebissen',
              'GIEẞEN': r'\bgieß|\bgoss|\bgegossen',
              'TREFFEN': r'\btr[ei]ff|\btr[aä]f|\bgetroffen',
              'RUFEN': r'\bruf|\brief|gerufen',
              'GERATEN': r'\bger[aä]t|\bgeriet|\bgeraten',
              'BRATEN': r'\bbr[aä]t|\bbriet|\bgebraten',
              'TRETEN': r'\btr[ea]t|\btritt|\bgetreten',
              '_TRETEN': r'tr[ea]t|tritt|getreten',
              'SITZEN': r'\bsitz|\bs[aä]ß|\bgesessen',
              'BITTEN': r'\bbitte|\bb[aä]t|gebeten',
              'ESSEN': r'\besse|\bisst|\b[aä]ß|\bgegessen',
              'FRESSEN': r'\bfresse|\bfrisst|\bfr[aä]ß|\bgefressen',
              'MESSEN': r'\bmesse|\bmisst|\bm[aä]ß|\bgemessen',
              'STREICHEN': r'\bstreich|\bstrich|\bgestrichen',
              'GLEICHEN': r'\bgleich|\bglich|\bgeglichen',
              '_WEICHEN': r'weich|wich|gewichen',
              'ERLEIDEN': r'\berleid|\berlitt|\berlitten',
# Inseparable prefixes
              'BEHALTEN': r'\bbeh[aä]lt|\bbehielt',
              'BEGEBEN': r'\bbeg[eiaä]b|\bbegeben',
              'BENEHMEN': r'\bbenimm|\bben[aäe]hm|\bbenommen',
              'BEKOMMEN': r'\bbekomm|\bbek[aä]m|\bbekommen',
              'VERLASSEN': r'\bverl[aä]ss|\bverließ|\bverlassen',
              'BEFINDEN': r'\bbef[iaä]nd|\bbefunden|\bbefunden',
              'ERGREIFEN': r'\bergreif|\bergriff|\bergriffen',
              'ERWEISEN': r'\berweis|\berwies|\berwiesen',
              'VERGLEICHEN': r'\bvergleich|\bverglich|\bverglichen',
              'VERGIEẞEN': r'\bvergieß|\bvergoss|\bvergossen',
              'VERDERBEN': r'\bverd[eiaüo]rb|\bverdorben', # o. verderbt
# Inseparable prefixes and present/past differ only by a vowel
              'ERFAHREN': r'\berf[aäuü]|\berfahren',
# Infinitive not included in other patterns
              'WOLLEN': r'\bwill|\bwollte|\bgewollt|\bwollen',
              'MÜSSEN': r'\bmuss|\bmüsste|\bgemusst|\bmüssen',
              'KÖNNEN': r'\bkann|\bk[oö]nnt|\bgekonnt|\bkönnen',
              '_KÖNNEN': r'kann|k[oö]nnt|gekonnt|können',
              'WISSEN': r'\bweiß|\bwisse|\bw[üu]sste|\bgewusst',
# Irregular
              'HABEN': (r'\bhab(e\b|t?\b|end?\b|es?t\b)|\bhas?t\b|'
                        r'\bh[aä]tte(\b|s?t\b|en\b)|'
                        r'\bgehabt'),
              '_HABEN': r'habe|hast|hat|hätt|gehabt',
              'SEIN': (r'\bbin\b|\bb?ist\b|\bsind\b|\bwar(\b|s?t\b|en\b)|'
                       r'\bwär(e|s?t\b)|\bsei(\b|d\b|e)|\bgewesen'),
              'TUN': r'\btu\b|\btue|\btu(s)?t\b|\bt[aä]t|getan|\btu[tn]\b',
              'GLEICHTUN': (r'\btu\b|\btue|\btu(s)?t\b|\bt[aä]t|\bgetan|'
                      r'\btu[tn]\b|\bgleichtu|\bgleicht[aä]t|gleichgetan'),
              # Separable, but standard way will probably give lots of false
              # positive hits.
              'ZUTUN': (r'\btu\b|\btue|\btus?t\b|\bt[aä]t|getan|\btu[tn]\b'
                        r'\bzutu|\bzut[aä]t|\bzuzutun|\bzugetan'),
              'ANTUN': (r'\btu\b|\btue|\btus?t\b|\bt[aä]t|getan|\btu[tn]\b'
                        r'\bantu|\bant[aä]t|\banantun|\bangetan'),
# Present and preterite differ by only a vowel
              'SPRINGEN': r'\bspr[iäa]ng|\bgesprungen',
              '_SPRINGEN': r'spr[iäa]ng|gesprungen',
              'ZWINGEN': r'\bzw[iaä]ng|\bgezwungen',
              'SCHWINGEN': r'\bschw[iaä]ng|\bgeschwungen',
              'FAHREN': r'\bf[aäu]hr|\bgefahren', # omit conj 2 on purpose
              '_FAHREN': r'f[aäu]hr|gefahren', # omit conj 2 on purpose
              'SPRECHEN': r'\bspr[eiäa]ch|\bgesprochen',
              'VERSPRECHEN': r'\bverspr[eiäa]ch|\bversprochen',
              'STERBEN': r'\bst[eiaü]rb|\bgestorben',
              'STECHEN': r'\bst[eiäa]ch|\bgestochen',
              'BRECHEN': r'\bbr[eiäa]ch|\bgebrochen',
              '_BRECHEN': r'br[eiäa]ch|gebrochen',
              'ZERBRECHEN': r'\bzerbr[eiäa]ch|\bzerbrochen',
              'WINDEN': r'\bw[iaä]nd|\bgewunden',
              '_WINDEN': r'w[iaä]nd|gewunden',
              'FINDEN': r'\bf[iaä]nd|\bgefunden',
              '_FINDEN': r'f[iaä]nd|gefunden',
              'ERFINDEN': r'\berf[iaä]nd|\berfunden',
              'BINDEN': r'\bb[iaä]nd|\bgebunden',
              '_BINDEN': r'b[iaä]nd|gebunden',
              'SCHWIMMEN': r'\bschw[iaäoö]mm|\bgeschwummen',
              'GEWINNEN': r'\bgew[iaäö]nn|\bgewonnen',
              'WACHSEN': r'\bw[aäuü]chs|\bgewachsen',
              'WASCHEN': r'\bw[aäuü]sch|\bgewaschen',
              'GRABEN': r'\bgr[äauü]b|\bgegraben',
              '_GRABEN': r'gr[äauü]b|gegraben',
              'BEGRABEN': r'\bbegr[äauü]b|\bbegraben',
              'WERFEN': r'\bw[eiaü]rf|\bgeworfen',
              '_WERFEN': r'w[eiaü]rf|geworfen',
              'HELFEN': r'\bh[eiaüä]lf|\bgeholfen',
              'HEBEN': r'\bh[eouöü]b|\bgehoben',
              '_HEBEN': r'h[eouöü]b|gehoben',
              'BEKENNEN': r'\bbek[ea]nn',
              'BRENNEN': r'\bbrenn|\bbrannte|\bgebrannt',
              'NENNEN': r'\bnenn|\bnannte|\bgenannt',
              'RENNEN': r'\brenn|\brannte|\bgerannt',
              'KENNEN': r'\bkenn|\bkannte|\bgekannt',
              'VERBRENNEN': r'\bverbrenn|\bverbrannt',
              'LÜGEN': r'\bl[üoö]g|\bgelogen',
              'WENDEN': r'\bw[ea]nd|gewandt|\bgewendet',
              'VERWENDEN': r'\bverw[ea]nd|verwandt|\bverwendet',
              'TRINKEN': r'\btr[iaä]nk|\bgetrunken',
              'STINKEN': r'\bst[iaä]nk|\bgestunken',
              '_STINKEN': r'st[iaä]nk|gestunken',
              'VERSINKEN': r'\bvers[iaä]nk|\bversunken',
              'FANGEN': r'\bf[aäi]ng|\bgefangen',
              'EMPFANGEN': r'\bempf[aäi]ng|\bempfangen',
              '_FANGEN': r'f[aäi]ng|\bgefangen',
              'TRAGEN': r'\btr[aäü]g|\bgetragen',
              '_TRAGEN': r'tr[aäü]g|getragen',
              'SCHLAGEN': r'\bschl[aäüu]g|\bgeschlagen',
              '_SCHLAGEN': r'schl[aäüu]g|geschlagen',
              'VERSCHLAGEN': r'\bverschl[aäüu]g|\bverschlagen',
              'ZERSCHLAGEN': r'\bzerschl[aäüu]g|\bzerschlagen',
              'GEBEN': r'\bg[eiaä]b|\bgegeben',
              '_GEBEN': r'g[eiaä]b|gegeben',
              'WERDEN': r'\bw[eiuaü]rd|geworden|\bworden',
# selected weak verbs
              'STELLEN': r'\bstell|\bgestellt',
              'MACHEN': r'\bmach|\bgemacht',
              '_MACHEN': r'mach|gemacht',
              'RUHEN': r'\bruh|\bgeruht',
              # Separable
              'AUSRUHEN': (r'\bruh|\bgeruht|'
                          r'\bausruh|\bauszuruhen|\bausgeruht'),
              'MITZEICHNEN': (r'\bzeichne|\bgezeichnet|'
                          r'\bmitzeichne|\bmitzuzeichnen|\bmitgezeichnet'),
              'BAUEN': r'\bbau|\bgebaut',
              'STÖREN': r'\bstör|\bgestört',
              'PUPEN': r'\bpup|\bgepupt',
              'TUTEN': r'\btute?|\bgetutet',
              'FÜHLEN': r'\bfühl|\bgefühlt',
              'FÜHREN': r'\bführ|\bgeführt',
              # avoid just '\bnäh' because would match 'näher'
              'NÄHEN': r'\bnähe\b|\bnäh(s)?t\b|\bnähte|\bgenäht',
              'LIEBEN': r'\blieb|\bgeliebt',
              'ÄRGERN': r'\bärger|\bgeärgert',
              'STECKEN': r'\bsteck|\bgesteckt',
              '_STECKEN': r'steck|gesteckt',
              'SUCHEN': r'\bsuch|\bgesucht',
              'ZEIGEN': r'\bzeig|\bgezeigt',
              'REDEN': r'\bred|\bgeredet',
              '_REDEN': r'red|geredet',
              'SAGEN': r'\bsag|\bgesagt',
              'WAGEN': r'\bwag|\bgewagt',
              'NAGEN': r'\bnag|\bgenagt',
              'RAUCHEN': r'\brauch|\bgeraucht',
              'KRIEGEN': r'\bkrieg|\bgekriegt',
              'LEITEN': r'\bleit|\bgeleitet',
              'SETZEN': r'\bsetz|\bgesetz',
              '_SETZEN': r'setz|gesetz',
              'LEGEN': r'\bleg|\bgelegt',
              'LEBEN': r'\bleb|\bgelebt',
              'HOLEN': r'\bhol|\bgeholt',
              '_HOLEN': r'hol|geholt',
              'HAUEN': r'\bhau|\bgehaut',
              '_HAUEN': r'hau|gehaut',
              'TRAUEN': r'\btrau|\bgetraut',
              'KAUEN': r'\bkau|\bgekaut',
              'KLAUEN': r'\bklau|\bgeklaut',
              'SCHAUEN': r'\bschau|\bgeschaut',
              'REISEN': r'\breis|\bgereist',
              '_REISEN': r'reis|gereist',
              'KAUFEN': r'\bkauf|\bgekauft',
              'FEGEN': r'\bfeg|\bgefegt',
              '_FEGEN': r'feg|gefegt',
              'LECKEN': r'\bleck|\bgeleckt',
              'LERNEN': r'\blern|\bgelernt',
              'LOBEN': r'\blob|\bgeloben',
              'RÄUMEN': r'\bräum|\bgeräumt',
              'BADEN': r'\bbad|\bgebadet',
              'MALEN': r'\bmal[etse]|\bgemalt', # omit 'mal' b/c too similar
              # write out the common forms in full, since 'üb' would match
              # the very common 'über'
              'ÜBEN': r'\büb(\b|en?\b|s?t\b|te)|\bgeübt',
              # Separable, see ABASTEN note
              'AUSÜBEN': (r'\büb(\b|en?\b|s?t\b|te)|\bgeübt'
                  r'\bausüb(\b|e(nd?)?\b|s?t\b|te)|\bauszuüben|\bausgeübt'),
              'LÖSEN': r'\blös|\bgelöst',
              # Separable
              'AUFLÖSEN': (r'\blös|\bgelöst'
                           r'\bauflös|\baufzulösen|\baufgelöst'),
              '_LEGEN': r'leg|gelegt',
              # separable, but standard way of just searching for ab + aste
              # will probably give a lot of false positive hits, so add here
              # so \b can anchor the start.
              'ABASTEN': (r'\baste|\babaste|\babzuasten|\babgeastet'),
# Not a verb
              'SICH': (r'\b[Mm]ich\b|\b[Dd]ich\b|\b[Ss]ich\b|\b[Uu]ns\b|'
                      r'\b[Ee]uch\b|\b[Mm]ir\b|\b[Dd]ir\b'),
# Misc
              'MANUAL_REVIEW': ''
             }

# Prepositions and also 'es'
PREP_OR_ES_SET = frozenset(['auf','an','aus','außer','bei','gegenüber',
            'hinter','in','mit', 'nach','neben','seit','statt','trotz','über',
            'unter','von','vor','wegen','während','zwischen','zu','es'])

#------------------------------------------------------------------------------
# Classes
#------------------------------------------------------------------------------
@dataclass
class IdiomReadRec:
    '''Idiom information the worker threads need only read from.
    '''
    verb_search_cat: str = ''
    headword: str = ''
    regexes: list = field(default_factory=list)
    ic_regexes: list = field(default_factory=list)

@dataclass
class IdiomWriteRec:
    '''Idiom information the worker threads will need to write to.
    '''
    results: list = field(default_factory=list)
    ic_results: list = field(default_factory=list)

#------------------------------------------------------------------------------
# Globals
#------------------------------------------------------------------------------

# These are global for the multiprocessing for the reasons below. The
# encapsulation could be improved by putting the worker functions in their own
# file with these globals. This may be improved in the future.
# All these are initialized to a value that is not `None` in the `_worker_init`
# function that is called when the pool is created.
#
# The barrier should be global so that it can be initialized for each process
# before the process calls `_return_results`.
_RESULT_BARRIER = None
# These are global for efficiency reasons. The _IDIOM_READONLY won't change
# for each execution of a worker task so we choose not to send it as an
# argument for each task. The IDIOM_COUNTS will be updated for each worker task
# but we want it to maintain state across the task executions within each
# process. The alternative is sending the results out for each task, but this
# is too much interprocess communication and will be significantly slower.
#
# This is a list that the worker processes only need to read from. Each element
# is a 2-tuple and each element in the tuple is an `IdiomReadRec` object. Every
# row in the input idiom file is added to the list.
_IDIOM_READONLY = None
# Like above, but the workers will need to write to it. Each element in the
# tuple is a `IdiomWriteRec`.
_IDIOM_COUNTS = None
# Will be used to pass text to be written to the match file from the worker
# processes to the process that prints this text. This will remain `None` and
# will not be used if `count_regexes.match_file is None`.
_MATCH_Q = None

#------------------------------------------------------------------------------
# Functions
#------------------------------------------------------------------------------
def _all_caps_or_underscore(x):
    for char in x:
        if not (char.isupper() or char == 'ß' or char == '_'): return False
    return True

def _replace_sichdab_forms(x):
    '''Replace SICHD, SICHA, SICHB in strings with reflexive pronuons.
    '''
    ret_x = x
    ret_x = ret_x.replace('SICHD', r'([Ss]ich|[Dd]ir|mir|uns|euch)')
    ret_x = ret_x.replace('SICHA', r'([Ss]ich|[Dd]ich|mich|uns|euch)')
    ret_x = ret_x.replace('SICHB',
                          r'([Ss]ich|[Dd]ich|[Dd]ir|mich|mir|uns|euch)')

    if 'SICH' in ret_x:
        raise ValueError(f'SICH substitution not done in {x=})')

    return _add_caps(ret_x)

def _replace_sein_forms(x):
    '''Replace SEIN in strings with mein, sein, ihr, usw.xi

    Perhaps a bit confusing because 'SEIN' in all capital letters when the
    only token represents the verb and was already replaced. Any remaining
    'SEIN' in the regular expression will be replaced with common
    determiners.
    '''
    if 'SICH' in x:
        # since we use if, elif when processing
        raise ValueError(f'SEIN.. and SICH.. in same regex {x=} not supported')
    ret_x = x
    ret_x = ret_x.replace('SEINMA',    # masc. acc.
                          r'(\b[MDSmds]einen\b|\b[Ii]hren\b|\b[Uu]nseren\b)')
    ret_x = ret_x.replace('SEINMN',    # masc. nom.
                          r'(\b[MDSmds]ein\b|\b[Ii]hr\b|\b[Uu]nser\b)')
    ret_x = ret_x.replace('SEINNN',    # neut. nom.
                          r'(\b[MDSmds]ein\b|\b[Ii]hr\b|\b[Uu]nser\b)')
    ret_x = ret_x.replace('SEINNA',    # neut. acc.
                          r'(\b[MDSmds]ein\b|\b[Ii]hr\b|\b[Uu]nser\b)')
    ret_x = ret_x.replace('SEINFN',    # fem. nom.
                          r'(\b[MDSmds]eine\b|\b[Ii]hre\b|\b[Uu]nsere\b)')
    ret_x = ret_x.replace('SEINFA',    # fem. acc.
                          r'(\b[MDSmds]eine\b|\b[Ii]hre\b|\b[Uu]nsere\b)')
    ret_x = ret_x.replace('SEINPA',    # pl. acc.
                          r'(\b[MDSmds]eine\b|\b[Ii]hre\b|\b[Uu]nsere\b)')
    ret_x = ret_x.replace('SEINPD',    # pl. dat.
                          r'(\b[MDSmds]einen\b|\b[Ii]hren\b|\b[Uu]nseren\b)')
    ret_x = ret_x.replace('SEINMND',   # masc./neut. dat.
                          r'(\b[MDSmds]einem\b|\b[Ii]hrem\b|\b[Uu]nserem\b)')
    ret_x = ret_x.replace('SEING',     # gen.
                          r'(\b[MDSmds]einer\b|\b[Ii]hrer\b|\b[Uu]nserer\b)')
    ret_x = ret_x.replace('SEINMNG',   # masc./neut. gen.
                          r'(\b[MDSmds]eines\b|\b[Ii]hres\b|\b[Uu]nseres\b)')
    if 'SEIN' in ret_x:
        raise ValueError(f'SEIN substitution not done in {x=})')

    return _add_caps(ret_x)

def _add_da_and_caps(x):
    orig_val = x
    # aus -> \b([Dd]a?r)?[Aa]us\b
    # Since we are adding word boundaries to the end of the prep, handle
    # common contractions too
    # inspected cohorts to see if '`' needs to be handled in the
    # abbreviations. Does not seem necessary
    if x == 'an':
        x = 'a(n(s)?|m)'
    elif x == 'in':
        x = 'i(n(s)?|m)'
    elif x == 'bei':
        x = 'beim?'
    elif x == 'hinter':
        x = "hinter('?[ms])?"
    elif x == 'über':
        x = "über('?[mns])?"
    elif x == 'unter':
        x = "unter('?[mns])?"
    elif x == 'vor':
        x = "vor('?[ms])?"
    elif x == 'auf':
        x = "auf('?[mns])?"
    elif x == 'außer':
        x = 'außer[ms]?'
    elif x == 'nach':
        x = "nach('?[ms])?"
    elif x == 'von':
        x = 'vo[nm]'
    elif x == 'es':
        x = r'`s\b|\b[Ee]s\b'
        return x
    elif x == 'zu':
        x = 'zu[mr]?'
    start_vowel = re.compile('^[aeiouäöü]')
    starts_w_vowel = start_vowel.search(x)
    make_cap = '[' + x[0].upper() + x[0] + ']' + x[1:] + r'\b'
    #make_cap = '(^' + x[0].upper() + '|' + x[0] + ')' + x[1:] + r'\b'
    if starts_w_vowel:
        res = r'\b[Dd]a?r' + orig_val + r'\b|\b' + make_cap
    else:
        res = r'\b[Dd]a' + orig_val + r'\b|\b' + make_cap
    return res

def _add_caps(x):
    if x[0].isupper() or not x[0].isalpha():
        return r'\b' + x + r'\b'
    else:
        return r'\b[' + x[0].upper() + x[0] + ']' + x[1:] + r'\b'
        #return r'\b(^' + x[0].upper() + '|' + x[0] + ')' + x[1:] + r'\b'

def _ext_seq(seq, n):
    if seq:
        return seq + ' -> ' + str(n)
    else:
        return str(n)

# ctr and note_id are currently unused but might be wanted when debugging
def _process_one_re(headword, relist_as_str, prob_verb_stems,
                    idiom_readonly, idiom_counts, re_idx):
    if re_idx == 1:
        idiom_readonly.append((IdiomReadRec(),IdiomReadRec()))
        idiom_counts.append((IdiomWriteRec(),IdiomWriteRec()))

    if not relist_as_str or relist_as_str == 'EXCLUDE':
        return
        #return {'n_cum': 0, 'n_seq': '', 'n_ic_cum': 0, 'n_ic_seq': ''}
    re_list = relist_as_str.split('+')

    # The commented-out code is from a draft version of this program where
    # vectorized pandas functions were used, e.g. 3.5 hr run-time vs 2.75 hr.

    #result = df.text
    #ic_result = df.text
    #n_cum = 0
    #n_seq = ''
    #n_ic_cum = 0
    #n_ic_seq = ''
    re_list_len = len(re_list)
    last_man_rev = re_list and 'MANUAL_REVIEW' == re_list[-1].strip()
    last_non_sich_index = 0
    has_verb_form = False
    has_prob_verb_stem = False
    for list_pos, regex in enumerate(re_list):
        if regex.strip() not in ['MANUAL_REVIEW','SICH']:
            last_non_sich_index = list_pos

    for list_pos, regex in enumerate(re_list):
        regex = regex.strip()
        prob_verb_stem = (' ' not in regex
                    and (regex[0].islower()
                         or (regex[0:2] == r'\b' and regex[2].islower()))
                    and regex not in NOT_VERB_FRAGMENTS
                    and ( ( re_list_len > 1
                            or re_list_len > 2 and last_man_rev))
                    and list_pos == last_non_sich_index)
        if prob_verb_stem and not _all_caps_or_underscore(regex):
            if regex.upper() + 'EN' in VERB_FORMS:
                print (f'WARNING: {regex=} also in VERB_FORMS')
            _add_to_prob_verb_stems(prob_verb_stems, regex, headword)
        if regex and _all_caps_or_underscore(regex):
            if (regex[0] != '_' and regex != 'MANUAL_REVIEW'
                and not (' ' + regex.lower() in headword
                         or headword.startswith(regex.lower()))):
                pass
                #print(f'WARNING: Verb {regex} not found in {headword}.')
            if (regex[0] == '_' and regex != 'MANUAL_REVIEW'
                and (' ' + regex.lower() in headword
                         or headword.startswith(regex.lower()))):
                pass
                #print(f'WARNING: Verb {regex} found in {headword}.')
            if regex not in VERB_FORMS:
                raise ValueError(f'Missing entry for {regex} in VERB_FORMS')
                #print(f"ERROR: {regex}")

        if regex and regex in PREP_OR_ES_SET:
            regex = _add_da_and_caps(regex)
        elif regex in VERB_FORMS:
            if regex not in ['SICH','MANUAL_REVIEW']:
                has_verb_form = True
            regex=VERB_FORMS[regex]
        elif 'SEIN' in regex:
            regex = _replace_sein_forms(regex)
        elif 'SICHD' in regex or 'SICHA' in regex or 'SICHB' in regex:
            regex = _replace_sichdab_forms(regex)
        elif (regex
              and '[' not in regex and r'\b' not in regex
              and not prob_verb_stem):
            regex = _add_caps(regex)
        regex.replace('ß','(ß|ss)')

        if prob_verb_stem:
            has_prob_verb_stem = True

        # BITTEN + bett[el]' has both a verb in VERB_FORMS and a verb stem,
        # but if any other cases appear, want to warn.
        if (has_verb_form and has_prob_verb_stem
               and relist_as_str != 'BITTEN + bett[el]'):
            print(f'WARNING: {relist_as_str=} has verb in '
                  'VERB_FORMS and prob_verb_stem')
            verb_search_cat = 'dict'
        elif has_verb_form:
            verb_search_cat = 'dict'
        elif has_prob_verb_stem:
            verb_search_cat = 'stem'
        else:
            verb_search_cat = ''
        ir_rec = idiom_readonly[-1][re_idx-1]
        iw_rec = idiom_counts[-1][re_idx-1]
        ir_rec.headword = headword
        ir_rec.verb_search_cat = verb_search_cat
        ir_rec.regexes.append(re.compile(regex))
        ir_rec.ic_regexes.append(re.compile(regex, flags=re.IGNORECASE))
        iw_rec.results.append(0)
        iw_rec.ic_results.append(0)
        #result = result[ result.str.contains(regex)]
        #n_cum = len(result)
        #n_seq = _ext_seq(n_seq, n_cum)
        #ic_result = ic_result[ ic_result.str.contains(regex.strip(),
        #                                              case=False)]
        #n_ic_cum = len(ic_result)
        #n_ic_seq = _ext_seq(n_ic_seq, n_ic_cum)
    #print(f'{ctr}: {note_id=}, {headword=}, re{re_idx}, n={len(result)},
    #      f' n_ic={len(ic_result)}')
    #return {'n_cum': n_cum, 'n_seq': n_seq,
    #        'n_ic_cum': n_ic_cum, 'n_ic_seq': n_ic_seq}

def _fmt_one_output(idx_, idiom_readonly, idiom_counts, re_idx):
    #n_cum = 0
    n_cum = ''
    n_seq = ''
    #n_ic_cum = 0
    n_ic_cum = ''
    n_ic_seq = ''
    #i_rec = rl_entry[re_idx-1]
    ir_rec = idiom_readonly[idx_][re_idx-1]
    i_rec = idiom_counts[idx_][re_idx-1]
    for result in i_rec.ic_results:
        n_ic_cum = str(result)
        n_ic_seq = _ext_seq(n_ic_seq, n_ic_cum)
    for result in i_rec.results:
        n_cum = str(result)
        n_seq = _ext_seq(n_seq, n_cum)
    return {'n_cum': n_cum, 'n_seq': n_seq,
            'n_ic_cum': n_ic_cum, 'n_ic_seq': n_ic_seq,
            'verb_search_cat': ir_rec.verb_search_cat}

def _fmt_output(rl_entry, idiom_readonly, idiom_counts):
    return {'re1': _fmt_one_output(rl_entry,
                                   idiom_readonly, idiom_counts, 1),
            're2': _fmt_one_output(rl_entry,
                                   idiom_readonly, idiom_counts, 2)}

def _return_results(_):
    # Need to wait to ensure this is called on every pool process.
    # This is because even though we called .map with chunksize=1, this doesn't
    # guarantee each pool process gets 1 task. One could finish fast and get 2.
    if _MATCH_Q is not None:
        _MATCH_Q.put(None)
    _RESULT_BARRIER.wait()
    return _IDIOM_COUNTS

def _worker_init(barrier, match_q, idiom_readonly, idiom_counts):
    global _RESULT_BARRIER
    global _IDIOM_READONLY
    global _IDIOM_COUNTS
    global _MATCH_Q
    _RESULT_BARRIER = barrier
    _IDIOM_READONLY = idiom_readonly
    _IDIOM_COUNTS = idiom_counts
    _MATCH_Q = match_q

def _print_matches(match_file, n_cores, match_q, final_q):
    sentinels_found = 0
    with open(match_file, 'w', encoding='utf-8') as f:
        while True:
            val = match_q.get()
            if val is None:
                sentinels_found += 1
                if sentinels_found == n_cores:
                    final_q.put(None)
                    break
            else:
                f.write(val)

# TODO: maybe in the future we will have the line_generator yield
# a file_index and/or line_number as well
#def _process_file_and_line(x):
    #_process_corpus_row(x[1].split('\t')[1])
#    _process_corpus_row(x)

def _process_corpus_row(x):
    #x_list = x.split(' ', maxsplit=1)
    #wgt = int(x_list[0])
    #text = x_list[1]
    for idx, row_rec in enumerate(_IDIOM_READONLY):
        for idx2, i_rec in enumerate(row_rec):
            case_sensitive_still_match = True
            for re_idx, regex in enumerate(i_rec.ic_regexes):
                len_results = len(i_rec.ic_regexes)
                if regex.search(x):
                    _IDIOM_COUNTS[idx][idx2].ic_results[re_idx] += 1
                    if (case_sensitive_still_match
                        and i_rec.regexes[re_idx].search(x)):
                        if (idx2 == 0 and re_idx + 1 == len_results
                           and _MATCH_Q is not None):
                            _MATCH_Q.put(
         f'{_IDIOM_READONLY[idx][idx2].headword}\t{x}')
                        _IDIOM_COUNTS[idx][idx2].results[re_idx] += 1
                    else:
                        case_sensitive_still_match = False
                else:
                    break

def _process_idiom(headword, re1, re2, prob_verb_stems, idiom_readonly,
                   idiom_counts):
    _process_one_re(headword, re1, prob_verb_stems,
                    idiom_readonly, idiom_counts, 1)
    _process_one_re(headword, re2, prob_verb_stems,
                    idiom_readonly, idiom_counts, 2)
    #return {'re1': process_one_re(ctr, note_id, headword, re1, 1),
    #        're2': process_one_re(ctr, note_id, headword, re2, 2)}

def _add_to_prob_verb_stems(prob_verb_stems, regex, headword):
    try:
        prob_verb_stems[regex].append(headword)
    except KeyError:
        prob_verb_stems[regex] = [headword]

def _write_prob_verb_stems(prob_verb_stems, pvs_output_file):
    pvs_df = pd.DataFrame.from_dict(prob_verb_stems, orient='index')
    pvs_df = pvs_df.sort_index()
    pvs_df.to_csv(pvs_output_file, sep='\t', quoting=csv.QUOTE_MINIMAL)

def _reduce_counts(rcvd, idiom_counts):
    '''Add the results from a worker to total.
    '''
    for idx1, idiom_row in enumerate(rcvd):
        for idx2, irec in enumerate(idiom_row):
            for idx3, val in enumerate(irec.results):
                idiom_counts[idx1][idx2].results[idx3] += val
            for idx3, val in enumerate(irec.ic_results):
                idiom_counts[idx1][idx2].ic_results[idx3] += val

def default_line_generator(corpus_files, max_rows_per_file):
    all_file_ctr = 0
    #for file_index, file in enumerate(corpus_files):
    for file in corpus_files:
        with open(file, encoding='utf-8') as f:
            for ctr, line in enumerate(f):
                if (max_rows_per_file is not None
                    and ctr >= max_rows_per_file): break
                if all_file_ctr % 1000 == 0:
                    print(f"Input line: {all_file_ctr}")
                all_file_ctr += 1
                #yield file_index, line
                yield line

def count_regexes(df, output_file, chunksize, n_cores=None,
                  line_generator=None,
                  corpus_files=None, max_rows_per_file=None,
                  pvs_output_file=None, match_file=None):
    '''Count regular expression combinations in corpora.

    This is the public interface to this module. See the module docstring
    for a description of module functionality.

    Parameters
    ----------
    df : pandas.DataFrame
        Data frame containing the idioms. This should contain the variables
        `headword`, `re1`, and `re2`. It should not contain a variable
        `_counter` as this will be temporarily created to store the sort
        order at input.
    output_file : str
        File name of output file. The output is tab-delimited with no
        quoting. It will be the `df` data frame plus the columns
        `verb_search_cat_N`, `n_cum_N`, `n_seq_N`, `n_ic_cum_N`,
        `n_ic_seq_N`, where `N` is replaced by 1 and 2.
    n_cores : int (>0) or None [default]
        Number of cores to use for multiprocessing the input corpus files.
        A value of 0 is not currently supported. If `None`, this
        will be set to `os.process_cpu_count()` which (at the time of this
        writing) is also the default `multiprocessing.Pool()` would use if
        `None` were passed. Note that if `match_file` (see below) is not
        `None` then an additional process will be started to write the
        matches that is not counted in the value of this parameter.
    chunksize : int
        Chunk size to pass to `multiprocessing.imap_unordered`, which
        distributes the tasks among the `n_cores` processes.
    line_generator : Callable[] or None
        A generator that takes no arguments and yields lines of text from
        the input files. If `None`, the default generator iterates over
        `corpus_files` and reads `max_rows_per_file` from each, with a
        status message printed every 1000 lines to standard output.
        See `default_line_generator` for details.
    corpus_files : iterable[str]
        This is an iterable container of file names containing the text
        from the corpus (e.g., a list[str] or tuple[str]). This is only
        used if `line_generator is None`.
    max_rows_per_file : int or None
        The maximum number of rows to read from each file in
        `corpus_files`. This is used if `line_generator is None`. This can
        also be set to 0 to suppress the check we perform `os.access`
    pvs_output_file : str or None
        File name of output file that contains a list of the regular
        expressions that are probably verb stems. If `None`, the file will
        not be created.
    match_file : str or None
        If not `None`, then all matches for the `re1` case-insenstive
        pattern will be written to this file.

    Results
    -------
    Nothing is returned (so an implicit `None`)
    '''

    #
    if line_generator is None and corpus_files is None:
        raise ValueError('count_regexes: `line_generator` or '
                         '`corpus_files` must be set')
    elif line_generator is None:
        line_generator = partial(default_line_generator,
                                 corpus_files, max_rows_per_file)
    else:
        if corpus_files is not None:
            print('WARNING: `corpus_files` will be ignored since '
                  '`line_generator` was set.')
        if max_rows_per_file is not None:
            print('WARNING: `max_rows_per_file` will be ignored since '
                  '`line_generator` was set.')

    if n_cores is None:
        n_cores = os.process_cpu_count()

    if n_cores == 0:
        raise ValueError('`n_cores == 0` not currently supported.')

    result_barrier = multiprocessing.Barrier(n_cores)

    # match_q: for text lines sent from workers to writer
    # final_q: for writer to tell main thread it is done
    if match_file is None:
        match_q = None
        final_q = None
    else:
        match_q = multiprocessing.Queue()
        final_q = multiprocessing.Queue()

    if '_counter' in df:
        raise ValueError('`_counter` already in input data frame')
    else:
        df['_counter'] = range(len(df))
    if df.headword.duplicated().any():
        print(df.headword[df.headword.duplicated()])
        raise ValueError('count_regexes: Duplicate headwords in `df`')

    # Sort by re1 to increase likelihood of cache hits.
    df = df.sort_values(['re1','_counter'])

    prob_verb_stems = {}
    idiom_readonly = []
    idiom_counts = []
    for row in df[['headword','re1','re2']].values:
        _process_idiom(headword=row[0], re1=row[1], re2=row[2],
                       prob_verb_stems=prob_verb_stems,
                       idiom_readonly=idiom_readonly,
                       idiom_counts=idiom_counts)
    if pvs_output_file is not None:
        _write_prob_verb_stems(prob_verb_stems, pvs_output_file)

    if match_q is not None:
        writer = multiprocessing.Process(target=_print_matches,
                        args=(match_file, n_cores, match_q, final_q))
        writer.start()

    if n_cores != 0:
        with multiprocessing.Pool(processes=n_cores,
                 initializer=_worker_init,
                 initargs=(result_barrier, match_q,
                           idiom_readonly, idiom_counts)) as pool:
            for _ in pool.imap_unordered(_process_corpus_row,
                            line_generator(), chunksize=chunksize):
                pass
            for counts in pool.imap_unordered(_return_results,
                                              [0]*n_cores, chunksize=1):
                _reduce_counts(counts, idiom_counts)

            if match_q is not None:
                _ = final_q.get()
                writer.join()
    else:
        pass
        #for line in line_generator():
        #    _process_corpus_row(line)

        #if match_q is not None:
        #    _ = final_q.get()
        #    writer.join()

    ret_val = [ _fmt_output(x, idiom_readonly, idiom_counts)
                for x in range(len(idiom_counts)) ]

    varlist = ['verb_search_cat','n_cum','n_seq','n_ic_cum','n_ic_seq']
    indices = ['1','2']
    for ix in indices:
        for var in varlist:
            df[var + '_' + ix] = [ x['re' + ix][var] for x in ret_val]

    df = df.sort_values(['_counter'])
    df = df.drop('_counter', axis=1)
    df.to_csv(output_file, sep='\t', quoting=csv.QUOTE_NONE, index=False)


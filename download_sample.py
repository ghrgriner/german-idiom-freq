#    Download, sample, and sort zipped corpus files
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

'''Download, sample, and sort corpus files.

   The public interface is `download_sample`. See that function for details.
'''

from datetime import datetime, timezone
from io import BytesIO
import gc
import random
import os
import re
import zipfile

import requests
import sys

#-----------------------------------------------------------------------------
# Functions
#-----------------------------------------------------------------------------
#def get_zip_fs_dict(fs_):
#    return {i: fs_.read(i) for i in fs_.namelist()}

def _sanitize_filename(filename):
    r'''Sanitize filename.

    Remove all characters from input filename, except any alphanumeric
    (r'\w'), or any characters [_. -/\()] (not including the brackets).
    Then, keep removing '../' and r'..\' until the result is unchanged.
    '''
    replace_re = re.compile(r'^\.\./|\.\.\\')
    filename = re.sub(r'[^\w_. -/\\)(]', '', filename)
    new_filename = ''
    while True:
        new_filename = replace_re.sub('', filename)
        if filename == new_filename:
            break
        else:
            filename = new_filename
    return new_filename

#filenames_for_testing = [r'C:\test', r'\/\C:\test',
#                         r'../bad_name', r'../good_name',
#                         r'path/to/file.txt', r'path\to\file.txt']
#print([_sanitize_filename(fn) for fn in filenames_for_testing])

def _extract_zip(zip_file,
                sampling_fraction,
                sort,
                output_dir,
                save_output):
    '''Output and/or just report on the output files in the zip file.

    If `save_output=True`, the files in the zip file will be sampled and
    sorted based on the `sampling_fraction` and `sort` parameters. The
    input/output lines will be counted and returned. If `save_output=False`
    the return value is the same, but the sampled/sorted file is not saved.

    Parameters
    ----------
    zip_file : zipfile.ZipFile
        The zip file.
    sampling_fraction, sort, output_dir, save_output :
        These are passed to this function by `download_sample`. See the
        `download_sample` documentation for details.

    Returns
    -------
    A dictionary containing the following lists (one item per output file):
    - original_output_filenames: The output filename as stored in the .zip
      file, before any sanitization was done.
    - output_filenames: Output filenames that will be or were used. These
      are sanitized versions of `original_output_filenames`.
    - input_lines: Input lines in each file, before sampling.
    - output_lines: Output lines in each file (i.e., after applying
         sampling). This can still be
    '''
    output_filenames =  []
    original_output_filenames =  []
    input_lines = []
    output_lines = []
    for output_index, filename in enumerate(zip_file.namelist()):
        sorted_sample = []
        input_count = 0
        output_count = 0

        if isinstance(sampling_fraction, list):
            sample_this_file = sampling_fraction[output_index]
        else:
            sample_this_file = sampling_fraction

        if isinstance(sort, list):
            sort_this_file = sort[output_index]
        else:
            sort_this_file = sort

        if '..' in filename:
            raise ValueError( 'Extracted output name contains ".." '
                             f'({filename})')
        # Don't call myfile.read() - this reads all the contents into memory,
        # and then we need to read the whole thing (or a fraction thereof)
        # into memory again into `sorted_sample` so we can sort it.
        with zip_file.open(filename) as myfile:
            print(f'Sampling {sample_this_file*100}% of the file')
            for line in myfile:
                input_count += 1
                if random.random() <= sample_this_file:
                    output_count += 1
                    sorted_sample.append(line.decode().replace('\n',''))
        if sort_this_file:
            print('Sorting')
            sorted_sample.sort()
        original_output_filenames.append(filename)
        output_filename = _sanitize_filename(filename)
        output_filenames.append(output_filename)
        input_lines.append(input_count)
        output_lines.append(output_count)
        if not save_output:
            print(f'Output file: {output_filename}')
        else:
            print(f'Outputting to: {output_filename}')
            with open(os.path.join(output_dir, output_filename),
                                   'w', encoding='utf-8') as f:
                for line in sorted_sample:
                    print(line, file=f)

    if not save_output:
        print('Output not saved because save_output=False')
    return {'output_filenames': output_filenames,
            'original_output_filenames': original_output_filenames,
            'input_lines': input_lines,
            'output_lines': output_lines,
           }

def _print_log(parameters, ext_result):
    '''Print log with parameters used and results of extraction.
    '''
    output_dir = parameters['output_dir']
    utc_now = datetime.now(timezone.utc)
    utc_string = utc_now.strftime('%Y-%m-%dT%H:%M:%SZ')
    with open(os.path.join(output_dir, 'download_sample.log'),
              mode='w', encoding='utf-8') as file:
        file.write('download_sample parameters:\n')
        for key, value in parameters.items():
            file.write('    ' + key + ' = ' + str(value) + '\n')
        file.write('\nResults of file extraction:\n')
        for key, value in ext_result.items():
            file.write('    ' + key + ' = ' + str(value) + '\n')
        file.write('\nOther:\n')
        file.write('    Current working directory: ' + os.getcwd() + '\n')
        file.write('    Current time (UTC): ' + utc_string + '\n')

def make_zip_bytes(urls, chunk_size):
    any_local = any([url.startswith('file://') for url in urls])

    if any_local:
        from requests_file import FileAdapter  # pylint: disable=import-outside-toplevel
        s = requests.Session()
        s.mount('file://', FileAdapter())
        get_fn = s.get
    else:
        get_fn = requests.get

    bioc = BytesIO()
    for url_idx, url in enumerate(urls):
        if url != 'stdin':
            response = get_fn(url, stream=True)
            #gc.collect()
            response.raise_for_status()

            # Now you can iterate over the content
            for chunk_num, chunk in enumerate(
                               response.iter_content(chunk_size=chunk_size)):
                print(f'File: {url_idx}: retrieved '
                      f'{(chunk_num+1)*chunk_size/(1024*1024)}M')
                bioc.write(chunk)
            #bioc.write(req.content)
        else:
            bioc.write(sys.stdin.buffer.read())
    gc.collect()

    return bioc

def download_sample(urls,
                    output_dir,
                    input_zipped,
                    sampling_fraction=1,
                    seed=None,
                    sort=True,
                    save_output=False,
                    chunk_size=10485760):
    '''Download sample or corpus and optionally sample and sort.

    Make a zip file by concatenating the data in one or more urls. Then,
    optionally, sample and sort the data and save the output. The output
    filenames in the zip file are used but are sanitized to protect against
    path transversal vulnerabilities. As an additional check, we recommend
    users first run with `save_output=False` and check the planned output
    file names, although the name could change after the check is done.

    If any url in `urls` starts with 'file://', then the `requests_file`
    module will be imported and a FileAdapter will be used that supports
    reading from local files. Otherwise, the `requests` package is used.

    Parameters
    ----------
    urls : list[str]
        List of urls for download. The urls, when concatenated, should
        create a file in the `.zip` file format. More precisely, the
        concatenation should be readable by zipfile.ZipFile().
        If one of the urls is 'stdin', then standard input will be read for
        that position in the list. It would be very unusual to mix 'stdin'
        with other URLs, but there is nothing preventing this.
    output_dir : str
        Output directory name for the log file and (if requested) the saved
        data.
    input_zipped : bool (must be `True`)
        Indicates that the data in the concatenated urls give a zip file
        (as opposed to, for example, uncompressed text). This currently
        must be `True`.
    sampling_fraction : float or list[float] (default = 1)
        Indicates the proportion of input lines to keep. If a list is
        passed, the values will be applied to the corresponding file in the
        list returned by `ZipFile.namelist()` for the zip flie.
    seed : Value supported by random.seed()
        If not None, `random.seed` is called and the value is passed.
    sort : bool or list[bool] (default = False)
        Indicates whether the output file(s) should be sorted. If a list is
        passed, the values will be applied to the corresponding file in the
        list returned by `ZipFile.namelist()` for the zip file.
    save_output : bool (default=False)
        Indicates whether the extracted data should be saved. The default
        is `False` as a reminder that users should first check the output
        names that will be used. However, the code does perform some
        sanitization of the output names.
    chunk_size : int (default = 10485760 [= 10*1024*1024])
        Chunk size used when making a `get` request. All requests use
        `stream = True` and are therefore chunked.

    Returns
    -------
    True if any request failed and False otherwise.
    '''
    parameters = locals().copy()
    if seed is not None:
        random.seed(seed)

    if not input_zipped:
        raise ValueError('download_sample: Only `input_zipped=True` supported')

    bioc = make_zip_bytes(urls, chunk_size)
    print('Calling zipfile.ZipFile')
    zip_file = zipfile.ZipFile(bioc)

    #print('Sleeping')
    #while True: pass

    ext_result = _extract_zip(zip_file=zip_file,
                 sampling_fraction=sampling_fraction,
                 output_dir=output_dir, sort=sort,
                 save_output=save_output)
    _print_log(parameters, ext_result)
    return True


#!/usr/bin/env python2
"""
   awk 'NR % 4 == 0' your.fastq | python %prog [options]
guess the encoding of a stream of qual lines.
"""
import argparse
import sys
import optparse

RANGES = {
    'Sanger': (33, 73),
    'Solexa': (59, 104),
    'Illumina-1.3': (64, 104),
    'Illumina-1.5-HMS': (66, 105),
    'Illumina-1.5': (67, 104),
    'Illumina-1.8': (33, 74)
}


def get_qual_range(qual_str):
    """
    >>> get_qual_range("DLXYXXRXWYYTPMLUUQWTXTRSXSWMDMTRNDNSMJFJFFRMV")
    (68, 89)
    """

    vals = [ord(c) for c in qual_str]
    return min(vals), max(vals)


def get_encodings_in_range(rmin, rmax, ranges=RANGES):
    valid_encodings = []
    for encoding, (emin, emax) in ranges.items():
        if rmin >= emin and rmax <= emax:
            valid_encodings.append(encoding)
    return valid_encodings


def main():
    parser = argparse.ArgumentParser('guess fastq encoding')
    parser.add_argument('-n', '--number_lines',
                        required=False,
                        default=-1,
                        type=int,
                        help='number of qual lines to test default:-1'
                        'means test until end of file or until it it possible to '
                        ' determine a single file-type',
    )
    parser.add_argument('-f', '--input_file',
                        required=True,
                        help='fastq input file.',
    )
    args = parser.parse_args()
    input_file = args.input_file
    number_lines = args.number_lines

    print >> sys.stderr, "# reading qualities from stdin"

    gmin, gmax = 99, 0
    valid = []
    input_file_open = open(input_file, 'r')
    for i, line in enumerate(input_file_open):
        if (i + 1) % 4 != 0:
            continue
        lmin, lmax = get_qual_range(line.rstrip())
        if lmin < gmin or lmax > gmax:
            gmin, gmax = min(lmin, gmin), max(lmax, gmax)
            valid = get_encodings_in_range(gmin, gmax)
            if len(valid) == 0:
                print >> sys.stderr, "no encodings for range: %s" % str((gmin, gmax))
                input_file_open.close()
                sys.exit()
            if len(valid) == 1 and number_lines == -1:
                print "\t".join(valid) + "\t" + str((gmin, gmax))
                input_file_open.close()
                sys.exit()

        if number_lines > 0 and i > number_lines:
            print "\t".join(valid) + "\t" + str((gmin, gmax))
            input_file_open.close()
            sys.exit()

    if 'Illumina-1.8' in valid:
        print('Illumina-1.8')
        input_file_open.close()
        sys.exit()
    elif 'Illumina-1.5' in valid:
        print('Illumina-1.5')
        input_file_open.close()
        sys.exit()
    elif 'Illumina-1.5-HMS' in valid:
        print('Illumina-1.5-HMS')
        sys.exit()
    elif '' in valid:
        print('Illumina-1.3')
        sys.exit()
    elif 'Solexa' in valid:
        print('Solexa')
        sys.exit()
    elif 'Sanger' in valid:
        print('Sanger')
        sys.exit()
    else:
        print "\t".join(valid) + "\t" + str((gmin, gmax))
        sys.exit(1)


if __name__ == "__main__":
    import doctest
    if doctest.testmod(optionflags=doctest.ELLIPSIS | \
                                   doctest.NORMALIZE_WHITESPACE).failed == 0:
        main()

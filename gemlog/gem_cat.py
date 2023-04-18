import argparse
import pathlib
import shutil
import sys
import warnings

import numpy as np
import pandas as pd

from gemlog.core import _read_single, EmptyRawFile


with warnings.catch_warnings():
    warnings.simplefilter("ignore")


def gem_cat(input_dir, output_dir, ext="", cat_all=False):
    """
    Merge raw data files so that all contain GPS data.

    Translated from R code originally by Danny Bowman.

    Parameters
    ----------
    input_dir: str
        Raw gem directory.
    output_dir: str
        Path for renumbered and concatenated files.
    ext: str
        Extension of raw files to convert (normally the serial number; sometimes TXT for old Gems).
    cat_all: bool
        Override to concatenate all files.

    """

    input_dir = pathlib.Path(input_dir)
    if not input_dir.is_dir():
        raise Exception(f"Input path {input_dir} does not exist")

    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    gem_files = sorted(input_dir.glob(f"FILE[0-9][0-9][0-9][0-9].{ext}*"))

    has_gps = np.zeros(len(gem_files))
    counter = 0
    out_file = None
    try:
        serial_number = pd.read_csv(
            gem_files[0],
            delimiter=",",
            skiprows=4,
            nrows=1,
            dtype="str",
            names=["s", "SN"],
        ).SN[0]
    except:
        raise EmptyRawFile(gem_files[0])

    for k, gem_file in enumerate(gem_files):
        print(f"{k + 1} of {len(gem_files)}: {gem_file}")

        if cat_all:
            if out_file is None:
                out_file = output_dir / f"FILE9999.{serial_number}"
                shutil.copy(str(gem_file), str(out_file))
            else:
                append_file(gem_file, out_file, gem_files[k - 1])

            continue

        try:
            # Here and elsewhere, delimiter='\t' is used as a dummy to read each line as
            # a whole string (read_csv doesn't accept '\n')
            lines = pd.read_csv(gem_file, delimiter="\t", dtype="str", names=["line"])
        except:
            raise EmptyRawFile(gem_file)

        gps_grep = lines.line.str.contains("^G")

        # If no files have been processed yet and current file has no GPS data, skip it
        if sum(gps_grep) == 0 and out_file is None:
            continue

        # If this is the first file being processed, simply copy it and advance
        if out_file is None:
            out_file = output_dir / f"FILE{counter:04d}.{serial_number}"
            shutil.copy(str(gem_file), str(out_file))
            counter += 1
            has_gps[k] = 1
            continue

        if sum(gps_grep) > 0:
            has_gps[k] = 1
            if has_gps[k - 1] == 0:
                # If this isn't the first file being processed and it does have GPS data
                # but the previous file didn't, append it to the current outfile
                append_file(gem_file, out_file, gem_files[k - 1])
            else:
                # If this isn't the first file being processed and it has gps data and
                # the previous file did too, start a new outfile
                out_file = output_dir / f"FILE{counter:04d}.{serial_number}"
                shutil.copy(str(gem_file), str(out_file))
                counter += 1
        else:
            # If this isn't the first file being processed and there's no GPS data,
            # append it to the current outfile
            append_file(gem_file, out_file, gem_files[k - 1])


def append_file(infile, outfile, prev_infile):
    """
    Append the contents of a file ('infile') to the current outfile, adding to a previous
    file when either the previous or the current file lacked GPS timing data.

    Parameters
    ----------
    infile: `pathlib.Path` object
        Path to the file containing the current data.
    outfile: `pathlib.Path` object
        Path to the file to which data will be written.
    prev_infile: `pathlib.Path` object
        Path to the previous input file.

    """

    header = pd.read_csv(infile, delimiter=",", nrows=1, dtype="str", names=["line"]).line[0]

    # Read first data line of infile and convert it to an ADC reading difference
    # Find end of header and append the infile to the outfile past that point
    j = 0
    while True:
        linetype = pd.read_csv(
            infile, skiprows=j, delimiter="\t", nrows=1, dtype="str", names=["s"]
        ).s[0][0]
        if linetype == "D":
            break
        j += 1

    format = header[7:]
    if format in ["0.85C", "0.9", "0.91"]:
        p_start = int(_read_single(prev_infile, require_gps=False, version=format)["data"][-1, 1])

        # Adjust the data line and append it to the outfile
        s = pd.read_csv(infile, delimiter=",", nrows=1, skiprows=j, dtype="str", names=["c1", "c2"])
        with open(outfile, "a") as OF, open(infile, "r") as IF:
            OF.write(f"{s['c1'].iloc[0]},{int(s['c2'].iloc[0]) - p_start}\n")
            j += 1

    # Append the rest of the file
    with open(outfile, "a") as OF, open(infile, "r") as IF:
        for k, line in enumerate(IF):
            if k >= j:
                OF.write(line)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--inputdir", type=str, default="raw", help="Raw Gem directory")
    parser.add_argument(
        "-o",
        "--outputdir",
        type=str,
        default="raw_merged",
        help="Path for renumbered and concatenated files",
    )
    parser.add_argument(
        "-e", "--ext", type=str, default="", help="Extension of raw files to convert"
    )
    parser.add_argument("-a", "--cat_all", type=bool, default=False)
    args = parser.parse_args()

    input_dir = pathlib.Path(args.inputdir)
    if not input_dir.is_dir():
        print(
            f"Cannot find input data folder '{input_dir}'.\n"
            "Did you give the right folder name after -i?\n"
        )
        sys.exit(1)

    if next(input_dir.iterdir(), None) is None:
        print(f"Data folder '{input_dir}' does not contain any data files.")
        sys.exit(1)

    try:
        gem_cat(input_dir, args.outputdir, args.ext, args.cat_all)
    except EmptyRawFile as e:
        print(f"EmptyRawFile: '{e}'")
        print("'gem_cat' failed.")
        sys.exit(1)

    print("Files merged successfully.\nRemember: sample times in the output are NOT precise!")


if __name__ == "__main__":
    main()

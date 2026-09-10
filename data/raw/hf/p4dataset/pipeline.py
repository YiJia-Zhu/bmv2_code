#!/usr/bin/env python3
import argparse
import logging
import os
import shutil
import subprocess
from contextlib import contextmanager

from augmentp4.create_misc_ds import create_misc_ds
from construct_dataset import construct_dataset
from download_ds import clone_repo, download_dataset
from download_stackv2 import download_stack_files
from filter_bad_files import filter_bad_files
from filter_p4 import copy_files_with_extension
from filter_p416 import filter_p416
from update_ds import update_ds

STACKV2LINKSDS = "stackv2-links.json"
BC_DEDUP_URL = "https://github.com/bigcode-project/bigcode-dataset"
BC_DST_DIR = "bigcode-dataset"


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


@contextmanager
def cwd(path):
    # Taken from: https://stackoverflow.com/a/37996581
    oldpwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldpwd)


def create_augmentation_ds():
    with cwd("augmentp4"):
        miscds = "p4dataset-misc.json"
        print("### Creating misc dataset ... ###")
        subprocess.run(["python3", "create_misc_ds.py", os.path.join("..", miscds)])


def apply_minhash_patch(bcdir):
    # Define the source and destination paths for the patch file
    patch_source = os.path.join(bcdir, "minhash_deduplication.patch")
    target_dir = os.path.join(bcdir, "near_deduplication")
    patch_dest = os.path.join(target_dir, "minhash_deduplication.patch")

    # Copy the patch file to the target directory
    shutil.copy2(patch_source, patch_dest)

    with cwd(target_dir):
        subprocess.run(["git", "apply", "minhash_deduplication.patch"])


def main():
    parser = argparse.ArgumentParser(description="Dataset processing pipeline.")
    parser.add_argument("--rootdir", type=str, help="Root directory for datasets.")
    parser.add_argument("--stackv2ds", type=str, help="StackV2 dataset file.")
    parser.add_argument("--stackv2linksds", type=str, default=STACKV2LINKSDS,
                        help="StackV2 links dataset file; default is %(default)s.")
    parser.add_argument("--bigcode", type=str, help="Path to the bigcode repo.")
    parser.add_argument("--download", action="store_true", help="Search github for P4 repos and download them.")
    parser.add_argument("--skip-p4-filtering", action="store_true",
                        help="Skip the P4 filtering step.")
    parser.add_argument("--blacklist", help="Path to a file containing "
                        "blacklisted P4 files")
    parser.add_argument("--dedup-threshold", type=float, default=0.85,
                        help="Deduplication threshold, default is %(default)s.")


    args = parser.parse_args()

    setup_logging()

    os.makedirs(args.rootdir, exist_ok=True)

    github_repos = os.path.join(args.rootdir, "github_repos")
    if args.download:
        os.makedirs(github_repos, exist_ok=True)
        logging.info("Downloading P4 repos from github to '{github_repos}' ...")
        download_dataset(github_repos, ".github_token.txt")

    allp4files = os.path.join(args.rootdir, "allp4files")
    if not args.skip_p4_filtering:
        os.makedirs(allp4files, exist_ok=True)
        logging.info(f"Extracting P4 files to '{allp4files}' ...")
        copy_files_with_extension(github_repos, allp4files, "p4")

    rawdataset = os.path.join(args.rootdir, "file_extension_p4.jsonl")
    logging.info(f"Constructing initial dataset '{rawdataset}' ...")
    construct_dataset(allp4files, rawdataset)

    if not args.stackv2ds:
        stackv2ds = os.path.join(args.rootdir, "stackv2p4.jsonl")
        logging.info("Downloading Stack v2 files to '{stackv2ds}' ...")
        download_stack_files(args.stackv2linksds, stackv2ds)
    else:
        logging.info("Using Stack v2 dataset from '{args.stackv2ds}' ...")
        stackv2ds = args.stackv2ds

    combinedds = os.path.join(args.rootdir, "dataset+stackv2.jsonl")
    logging.info(f"Combining datasets to '{combinedds}' ...")
    update_ds(rawdataset, stackv2ds, combinedds)

    p416ds = os.path.join(args.rootdir, "p416dataset.jsonl")
    logging.info(f"Filtering P4-16 files to '{p416ds}' ...")
    filter_p416(combinedds, p416ds)

    filteredds = os.path.join(args.rootdir, "filteredgood.jsonl")
    logging.info(f"Filtering good files to '{filteredds}' ...")
    filter_bad_files(p416ds, filteredds, args.blacklist)

    if not args.bigcode:
        bigcode = os.path.join(args.rootdir, "bigcode-dataset")
        logging.info(f"Cloning BigCode dataset to '{bigcode}' ...")
        clone_repo(BC_DEDUP_URL, bigcode)
        apply_minhash_patch(bigcode)
    else:
        logging.info(f"Using BigCode dataset from '{args.bigcode}' ...")
        bigcode = args.bigcode

    dedupds = os.path.join(args.rootdir, "code.jsonl")
    mhdpath = os.path.join(bigcode,
                           "near_deduplication/minhash_deduplication.py")
    logging.info(f"Deduplicating dataset to '{dedupds}' ...")
    subprocess.run(["python3", mhdpath,
                    "--dataset", filteredds,
                    "--dedupjson", dedupds,
                    "--threshold", args.dedup_threshold])

    augds = os.path.join(args.rootdir, "networking_knowledge.jsonl")
    logging.info(f"Augmenting dataset to '{augds}' ...")
    create_misc_ds("augmentp4", augds)

    finalds = os.path.join(args.rootdir, "code+text.jsonl")
    logging.info(f"Creating full dataset at '{finalds}' ...")
    update_ds(dedupds, augds, finalds)


if __name__ == "__main__":
    main()

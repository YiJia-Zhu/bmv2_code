#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import requests
import os
from git import Repo
import sys


# Create a top-level logger for this module.
logger = logging.getLogger(__name__)


def search_repos(language, headers, per_page=100, page=1):
    """Search for repositories in a specific language."""
    query = f'https://api.github.com/search/repositories?q=language:{language}&per_page={per_page}&page={page}'
    logger.info("Searching for repos (language=%s, page=%d)...", language, page)
    response = requests.get(query, headers=headers)
    if response.status_code == 200:
        logger.info("Successfully retrieved page %d", page)
        return response.json()
    else:
        logger.error("Failed to retrieve page %d: %s", page, response.content)
        return None


def clone_repo(repo_url, clone_folder):
    """Clone a repository to a specified folder."""
    logger.info("Cloning repository '%s' into '%s'...", repo_url, clone_folder)
    try:
        Repo.clone_from(repo_url, clone_folder)
        logger.info("Done cloning '%s'", repo_url)
    except Exception as e:
        logger.error("Failed to clone %s: %s", repo_url, e)



def download_dataset(dstdir, token_path, language="P4"):
    try:
        with open(token_path, "rt") as fin:
            token = fin.read().strip("\n")
    except FileNotFoundError:
        logger.error("Could not find token file: %s", token_path)
        sys.exit(1)

    headers = {'Authorization': f'token {token}'}

    page = 1
    while True:
        result = search_repos(language, headers, page=page)
        if result and result['items']:
            for repo in result['items']:
                repo_url = repo['clone_url']
                fullname = repo["full_name"]

                clone_folder = os.path.join(dstdir, fullname)
                os.makedirs(clone_folder, exist_ok=True)
                clone_repo(repo_url, clone_folder)

            page += 1
        else:
            logger.info("No more repositories returned; stopping.")
            break


def main():
    parser = argparse.ArgumentParser(
        description="Download GitHub repositories for a specified language."
    )
    parser.add_argument(
        "destination",
        help="Destination directory where the repositories will be cloned."
    )
    parser.add_argument(
        "--token-file",
        default=".github_token",
        help="Path to the file containing the GitHub personal access token (default: .github_token)."
    )
    parser.add_argument(
        "--language",
        default="P4",
        help="Programming language to search for (default: P4)."
    )
    # Configure logging (you can adjust the log level and format as needed)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    args = parser.parse_args()

    # Call the main function that downloads the repos
    download_dataset(
        dstdir=args.destination,
        token_path=args.token_file,
        language=args.language
    )
if __name__ == "__main__":
    main()

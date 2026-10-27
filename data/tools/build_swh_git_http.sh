#!/usr/bin/env bash
# Build an OpenSSL HTTP helper locally, without replacing the system Git.
set -euo pipefail
TASK_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOLCHAIN_ROOT="$TASK_ROOT/data/raw/repositories/swh_full_history/toolchain"
mkdir -p "$TOOLCHAIN_ROOT"
cd "$TOOLCHAIN_ROOT"
if [[ ! -f git-2.51.0.tar.xz ]]; then
    curl --fail --location --proxy http://127.0.0.1:7892 --retry 4 --retry-all-errors \
        --connect-timeout 20 --max-time 900 --continue-at - \
        https://www.kernel.org/pub/software/scm/git/git-2.51.0.tar.xz -o git-2.51.0.tar.xz.part
    mv git-2.51.0.tar.xz.part git-2.51.0.tar.xz
fi
if [[ ! -d curl-dev ]]; then
    apt-get -o Acquire::http::Proxy=http://127.0.0.1:7892 \
        -o Acquire::https::Proxy=http://127.0.0.1:7892 download libcurl4-openssl-dev
    mkdir -p curl-dev
    dpkg-deb --extract libcurl4-openssl-dev_*.deb curl-dev
fi
if [[ ! -d git-2.51.0 ]]; then
    tar -xf git-2.51.0.tar.xz
fi
cd git-2.51.0
make -j16 NO_GETTEXT=YesPlease NO_TCLTK=YesPlease NO_PERL=YesPlease NO_PYTHON=YesPlease \
    NO_CURL_CONFIG=YesPlease \
    CURL_CFLAGS="-I$TOOLCHAIN_ROOT/curl-dev/usr/include/x86_64-linux-gnu" \
    CURL_LIBCURL=/usr/lib/x86_64-linux-gnu/libcurl.so.4 git-remote-http
mkdir -p "$TOOLCHAIN_ROOT/git-core"
for HELPER_PATH in /usr/lib/git-core/*; do
    HELPER_NAME="${HELPER_PATH##*/}"
    if [[ ! -e "$TOOLCHAIN_ROOT/git-core/$HELPER_NAME" ]]; then
        ln -s "$HELPER_PATH" "$TOOLCHAIN_ROOT/git-core/$HELPER_NAME"
    fi
done
for HELPER_NAME in git-remote-http git-remote-https git-remote-ftp git-remote-ftps; do
    ln -sfn "$TOOLCHAIN_ROOT/git-2.51.0/git-remote-http" "$TOOLCHAIN_ROOT/git-core/$HELPER_NAME"
done
ldd "$TOOLCHAIN_ROOT/git-2.51.0/git-remote-http" > "$TOOLCHAIN_ROOT/http-helper-linkage.txt"
sha256sum git-remote-http ../git-2.51.0.tar.xz > "$TOOLCHAIN_ROOT/http-helper-sha256.txt"
printf '%s\n' 'OpenSSL Git HTTP helper ready.'

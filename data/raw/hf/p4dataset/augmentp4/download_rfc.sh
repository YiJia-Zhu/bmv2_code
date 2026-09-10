#!/bin/bash

# Start of the range
start=700

# End of the range
end=3000

# Base URL for downloading RFC text files
baseUrl="https://www.rfc-editor.org/rfc/rfc"

# Directory where files will be saved
destDir="rfcs"

# Create the directory if it doesn't exist
mkdir -p "$destDir"

# Loop from start to end
for id in $(seq $start $end)
do
  # Construct the URL
  url="${baseUrl}${id}.txt"
  
  # Download the file and save it in the specified directory
  wget "$url" -O "${destDir}/rfc${id}.txt" || echo "Failed to download RFC ${id}"
  sleep 0.1
done

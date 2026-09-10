import requests
from bs4 import BeautifulSoup

# Function to fetch and extract text from a URL
def fetch_and_extract_text(url, counter):
    try:
        # Send a HTTP request to the URL
        response = requests.get(url.strip())
        # Ensure the request was successful
        response.raise_for_status()

        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all div elements with itemprop='text'
        divs = soup.find_all('div', itemprop='text')

        # Extract and print the text from each div
        text = ""
        for div in divs:
            text += div.get_text(strip=True) + "\n"

        file_name = f"discussions/{counter}.txt" 
        with open(file_name, 'w') as file:
            file.write(text)

    except Exception as e:
        print(f"Failed to process {url}: {e}")

# Read URLs from the file
with open('urls.txt', 'r') as file:
    urls = file.readlines()

counter = 0

# Process each URL
for url in urls:
    fetch_and_extract_text('https://forum.p4.org/t/' + url, counter)
    counter = counter + 1

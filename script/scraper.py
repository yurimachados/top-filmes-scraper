import requests
import time
import csv
import random
import concurrent.futures

from bs4 import BeautifulSoup

# Global Headers
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36 Edg/94.0.992.31'}

MAX_THREADS = 10

def extract_movies(soup):

    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')

    movie_table_rows = movies_table.find_all('li')
    movie_links = ['https://www.imdb.com' + movie.find('a')['href'] for movie in movie_table_rows]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)
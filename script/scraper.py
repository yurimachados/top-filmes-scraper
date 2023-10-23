import requests
import time
import csv
import random
import concurrent.futures

from bs4 import BeautifulSoup

# Global Headers
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36 Edg/94.0.992.31'}

MAX_THREADS = 10

def extract_movie_details(movie_link):
    global processed_count
    processed_count += 1
    time.sleep(random.uniform(0, 0.2))
    try:
        response = BeautifulSoup(requests.get(movie_link, headers=headers).content, 'html.parser')
        movie_soup = response
    except requests.exceptions.RequestException as e:
        print(f'Erro na solicitação HTTP: {e}')
    except Exception as e:
        print(f'Erro na análise do HTML: {e}')
        movie_soup = None


    if movie_soup is not None:

        title = movie_soup.find('h1', attrs={'class': 'sc-afe43def-0 hnYaOZ'}).get_text()
        popularity = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__popularity__score'}).get_text()
        rating = movie_soup.find('span', attrs={'class': 'sc-bde20123-1 iZlgcd'}).get_text()  if movie_soup.find('span', attrs={'class': 'sc-bde20123-1 iZlgcd'}) else None
        plot_text = movie_soup.find('span', attrs={'class': 'sc-466bb6c-0 kJJttH'}).get_text().strip() if movie_soup.find('span', attrs={'class': 'sc-466bb6c-0 kJJttH'}) else None


        # print(["Titulo: ", title,"Data: ", popularity, "Rating: ", rating,"Descrição: ", plot_text])

        with open('movies.csv', mode='a') as file:
            movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if all([title, popularity, rating, plot_text]):
                # print('Gravando dados no CSV', title, popularity, rating, plot_text)
                movie_writer.writerow([processed_count, title, popularity, rating, plot_text])
                print(f'O total de filmes processados:{processed_count}')  
            else:
                print("Dados ausentes, Não gravados no CSV", ["Titulo: ", title,"Data: ", popularity, "Rating: ", rating,"Descrição: ", plot_text])
    else: 
        print('problema ao ler HTML')


def extract_movies(soup):

    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')

    movie_table_rows = movies_table.find_all('li')
    movie_links = ['https://www.imdb.com' + movie.find('a')['href'] for movie in movie_table_rows]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)
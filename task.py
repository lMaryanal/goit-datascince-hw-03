from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import json
import re
import json
from bs4 import BeautifulSoup

def authors_page(url, page):
    response = requests.get(page)
    soup = BeautifulSoup(response.text, 'html.parser')
    authors_link_page = soup.select('div[class=quote] span a')
    for link in authors_link_page:
        author = {}
        url_auth = url + link['href']
        response = requests.get(url_auth)
        soup = BeautifulSoup(response.text, 'html.parser')
        author_details = soup.find('div', attrs= {"class":"author-details"})
        author["fullname"] = author_details.find('h3', attrs= {"class":"author-title"}).text
        author["born_date"] = author_details.find('p').find('span', attrs= {"class":"author-born-date"}).text
        author["born_location"] = author_details.find('p').find('span', attrs= {"class":"author-born-location"}).text
        author["description"] = author_details.find('div', attrs= {"class":"author-description"}).text.strip()
        authors.append(author)


def qoutes_page(page):
    response = requests.get(page)
    soup = BeautifulSoup(response.text, 'html.parser')
    for q in soup.select('div[class=quote]'):
        qoute = {}
        qoute["tags"] = [i.text for i in q.find('div', attrs= {"class":"tags"}).select("a")]
        qoute["author"] = q.select('span')[1].find('small', attrs= {"class":"author"}).text
        qoute["quote"] = q.find('span', attrs = {"class":"text"}).text
        qoutes.append(qoute)


def next_page(url):
    url_next = url
    yield url_next 
    for p in range(9):
        response = requests.get(url_next)
        soup = BeautifulSoup(response.text, 'html.parser')
        next = soup.select('div[class=col-md-8] nav ul li[class=next] a')
        url_next = url + next[0]['href']
        yield url_next 



if __name__ == "__main__":
    url = 'http://quotes.toscrape.com'
    authors = []
    qoutes = []
    for page in next_page(url):
        qoutes_page(page)
        authors_page(url, page)


    with open('authors.json', 'w', encoding='utf-8') as f:
        json.dump(authors, f, ensure_ascii=False, indent=4)  
    with open('qoutes.json', 'w', encoding='utf-8') as f:
        json.dump(qoutes, f, ensure_ascii=False, indent=4)  

        
    uri = "mongodb+srv://serbakmarana671:RjVz1wRKmAfcMz20@cluster0.xydhvoa.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client.ds_dz03 
    with open('authors.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        db.authors.insert_many(data)
    with open('qoutes.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        db.qoutes.insert_many(data)



    

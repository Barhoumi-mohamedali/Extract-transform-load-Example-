# importer ces deux modules bs4 pour sélectionner facilement les balises HTML
import requests 
from bs4 import BeautifulSoup

import urllib.request
import time

import json
import csv


filecsv = open('output.csv', 'w',encoding='utf8')
file = open('output.json','w',encoding='utf8')
# Fixer le lien sur lequel on doit extraire les données.
url = 'https://www.usine-digitale.fr/cybersecurite/'
file.write('[\n') ## ouvrir object Json
data = {}
csv_columns = ['name','article']

## pourcourir les pages.
for page in range(5):
    print('---', page, '---')
    r = requests.get(url + str(page)+'/') 
    print(url + str(page))
    soup = BeautifulSoup(r.content, "html.parser") ##charger le contenu de page avec html parser  ### BeautifulSoup est utilisé pour obtenir la structure HTML à partir de la réponse aux demandes 
    ancher=soup.find_all('div',class_='texteContenu3') ## Permet de chercher cet portion de code dans la page html
    writer = csv.DictWriter(filecsv, fieldnames=csv_columns)
    i=0
    writer.writeheader()

## Réccupérer les éléments à partir la portion de code HTML.
    for pt in  ancher:
        name=pt.find('h2',{'class':'titreType2'})
        article=pt.find('p',{'class':'chapoType1'})
        print (name.text)
        print (article.text)
        writer.writerow({'name': name.text.replace('  ', '').strip('\r\n'), 'article': article.text})
        data['name'] =name.text.replace('  ', '').strip('\r\n')
        data['article'] =article.text

        json_data = json.dumps(data,ensure_ascii=False) 
        file.write(json_data)

file.write(",\n")     
        
file.write("\n]")
filecsv.close()
file.close()

import requests
import urllib.request
import time
import json
import threading
import csv
from pandas import *
import numpy as np
import statistics
from bs4 import BeautifulSoup
import scipy.stats

baseUrl = 'https://www.pro-football-reference.com/'

stats = {}
ind = []

def mean_confidence_interval(data, confidence=0.95):
    a = 1.0 * np.array(data)
    n = len(a)
    m, se = np.mean(a), scipy.stats.sem(a)
    h = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
    return m, m-h, m+h

def calculatePoints(game):
    points = 0
    points += game.get('pass_td', 0) * 4
    points += game.get('pass_yds', 0) / 25
    points += (0, 3)[game.get('pass_yds', 0) >= 300]
    points += game.get('pass_int', 0)
    points += game.get('rush_td', 0) * 6
    points += game.get('rush_yds', 0) / 10
    points += (0, 3)[game.get('rush_yds', 0) >= 100]
    points += game.get('rec_td', 0) * 6
    points += game.get('rec_yds', 0) / 10
    points += (0, 3)[game.get('rec_yds', 0) >= 100]
    points += game.get('rec', 0)
    points -= game.get('fumbles_lost', 0)
    #todo special teams td
    return points

def individualDownload(a, count):
    a = a[0]
    href = a['href']
    name = a.text
    gamelogUrl = baseUrl + href[:-4] + '/gamelog/'
    response = requests.get(gamelogUrl)
    soup = BeautifulSoup(response.text, 'html.parser')
    t = soup.find('table')
    tbod = t.find('tbody')
    tr = tbod.find_all('tr')
    for row in tr:
        a = row.find_all('a')
        if len(a) == 0:
            continue
        columns = row.find_all('td')
        game = {}
        for column in columns:
            try:
                game[column.attrs['data-stat']] = float(column.string.replace('%',''))
            except:
                if column.attrs['data-stat'] == 'game_location':
                    game['game_location'] = (0, 1)[column.string == None]
                continue
        game['fantasy_points'] = calculatePoints(game)
        stats.setdefault(name,[]).append(game)
    fPoints = [ g['fantasy_points'] for g in stats[name] ]
    
    fPoints = fPoints[-16:]

    std = np.std(fPoints, ddof=1)
    mean, lower, upper = mean_confidence_interval(fPoints)
    ind.append([name, mean, lower, upper])



def main():
    threads = []
    count = 0
    url1 = baseUrl + 'years/'
    url2 = '/rushing.htm'#todo other players
    for year in range(2019, 2020):
        url = url1 + str(year) + url2
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        t = soup.find('table')
        tbod = t.find('tbody')
        tr = tbod.find_all('tr')
        count += len(tr)
        count2 = 0
        for row in tr:
            count2 += 1
            if count2 == 50:
                break
            a = row.find_all('a')
            if len(a) > 0:
              el = row.select("td[data-stat='pos']")[0].text
              #if el == 'RB' or el == 'rb':
              t = threading.Thread(target=individualDownload, args=(a, count))
              t.start()
              threads.append(t)


    for t in threads:
        t.join()

    ind.sort(key=lambda x: x[2], reverse=True)
    indexCounter = 0
    print(DataFrame(ind))
    #for row in ind:
    #    print(str(indexCounter) + str(row))
    #    indexCounter += 1

if __name__ == "__main__":
    main()

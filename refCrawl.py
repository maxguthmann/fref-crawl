import requests
import urllib.request
import time
import json
import threading
import csv
from pandas import *
import numpy as np
from bs4 import BeautifulSoup
import scipy.stats
import sqlite3



baseUrl = 'https://www.pro-football-reference.com/'

stats = {}
ind = []

lock = threading.Lock()

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
    conn = sqlite3.connect('nfl.db')
    c = conn.cursor()
    a = a[0]
    href = a['href']
    name = a.text

    lock.acquire()
    c.execute('SELECT * FROM games WHERE name = ?', [name])
    conn.commit()
    lock.release()
    if len(c.fetchall()) != 0:
        return

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
        game['name'] = name
        for column in columns:
            try:
                game[column.attrs['data-stat']] = float(column.string.replace('%',''))
            except:
                if column.attrs['data-stat'] == 'game_location':
                    game['game_location'] = (0, 1)[column.string == None]
                else:
                    continue
            #print(column.attrs['data-stat'])
        #print(game)
        game['fantasy_points'] = calculatePoints(game)
        stats.setdefault(name,[]).append(game)
        columns = ', '.join(game.keys())
        placeholders = ':'+', :'.join(game.keys())
        query = 'INSERT INTO games (%s) VALUES (%s)' % (columns, placeholders)
        #print(query)
        lock.acquire()
        c.execute(query, game)
        conn.commit()
        lock.release()

    lock.acquire()
    conn.close()
    lock.release()

    fPoints = [ g['fantasy_points'] for g in stats[name] ]
    #print(stats[name][0])
    fPoints = fPoints[-16:]
    mean, lower, upper = mean_confidence_interval(fPoints)
    ind.append([name, mean, lower, upper])

def createGamesTable():
    conn = sqlite3.connect('nfl.db')
    c = conn.cursor()
    c.execute('DROP TABLE IF EXISTS games')
    c.execute('''CREATE TABLE games
    (name text,
    year_id real,
    game_num real,
    week_num real,
    age real,
    game_location real,
    rush_att real,
    rush_yds real,
    rush_yds_per_att real,
    rush_td real,
    targets real,
    rec real,
    rec_yds real,
    rec_yds_per_rec real,
    rec_td real,
    catch_pct real,
    rec_yds_per_tgt real,
    pass_cmp real,
    pass_att real,
    pass_cmp_perc real,
    pass_yds real,
    pass_td real,
    pass_int real,
    pass_rating real,
    pass_sacked real,
    pass_sacked_yds real,
    pass_yds_per_att real,
    pass_adj_yds_per_att real,
    all_td real,
    scoring real,
    fumbles real,
    fumbles_lost real,
    fumbles_forced real,
    fumbles_rec real,
    fumbles_rec_yds real,
    fumbles_rec_td real,
    offense real,
    off_pct real,
    defense real,
    def_pct real,
    special_teams real,
    st_pct real,
    kick_ret real,
    kick_ret_yds real,
    kick_ret_td real,
    kick_ret_yds_per_ret real,
    sacks real,
    punt_ret real,
    punt_ret_td real,
    punt_ret_yds real,
    punt_ret_yds_per_ret real,
    tackles_solo real,
    tackles_assists real,
    tackles_combined real,
    tackles_loss real,
    qb_hits real,
    two_pt_md real,
    def_int real,
    punt real,
    punt_yds real,
    punt_blocked real,
    punt_yds_per_punt real,
    def_int_yds real,
    def_int_td real,
    pass_defended real,
    xpm real,
    xpa real,
    safety_md real,
    fantasy_points real)''')
    conn.commit()
    conn.close()

def main():
    createGamesTable()
    threads = []
    count = 0
    url1 = baseUrl + 'years/'
    url2s = ['/passing.htm', '/scrimmage.htm']#todo other players
    for url2 in url2s:
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
                #if(count2 == 1):
                #    break
                count2 += 1
                if url2 == '/passing.htm':
                    if count2 == 25:
                        break
                else:
                    if count2 == 200:
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
    print(DataFrame(ind).to_string())

if __name__ == "__main__":
    main()

# -*- coding:utf-8 -*-
import requests
from bs4 import BeautifulSoup
import MySQLdb
import time


class BaiduAlbums():
    def __init__(self):
        self.host = '101.200.159.42'
        self.user = 'java'
        self.pw = 'inspero'
        self.database = 'musicnew'
        self.time_stamp = []
        self.database = MySQLdb.connect(self.host, self.user, self.pw, self.database, charset='utf8')
        self.cursor = self.database.cursor()
        self.cursor.execute('select version()')
        data = self.cursor.fetchone()
        print int(time.time()), 'Database version : %s' % data
        del data

    def get_urls(self):
        '''
        :return:urls to be requested
        '''
        urls = []
        for i in range(0, 81, 10):
            temp = 'http://music.baidu.com/album/shoufa?order=time&style=all&start=%d&size=10&third_type=' % i
            urls.append(temp)
        return urls

    def get_one_page(self, url):
        '''
        :param url:start url to be requested
        :return: one page data
        '''
        page_list = []
        response = requests.get(url=url).content
        soup = BeautifulSoup(response)
        album_info_list = soup.find_all('div', class_='album-info')
        for album in album_info_list:
            try:
                s_time = album.find_all('span', class_='time')
                time_var = s_time[0].text
                album_href = album.a['href']
                album_id = album.a['href'][7:]
                album_title = album.a['title']
                album_name = album.a.text
                author_list = album.find_all('span', class_='author_list')
                a_a = author_list[0].find_all('a')
                author_href = ''
                author_text = ''
                for i, a in enumerate(a_a):
                    if i == 0:
                        author_href += a_a[i].get('href')
                        author_text += a_a[i].text
                    elif i >= 1:
                        author_href += '+' + a_a[i].get('href')
                        author_text += '+' + a_a[i].text
                    else:
                        pass
                temp_dict = {'time_var': time_var, 'album_id': album_id, 'album_title': album_title,
                             'album_href': album_href, 'album_name': album_name, 'author_href': author_href,
                             'author_text': author_text}
                page_list.append(temp_dict)
            except Exception, e:
                fn = open('error.log', 'a')
                fn.write(str(e))
                fn.write('\t')
                fn.write(album.a['href'][7:])
                fn.write('\n')
                fn.flush()
                fn.close()
        return page_list

    def get_processed_list(self, urls):
        '''
        :param urls:all urls to be requested
        :return: all processed list that will be inserted into mysql
        '''
        all_pages_list = []
        processed_list = []
        exist_id = []
        for u in urls:
            all_pages_list += self.get_one_page(u)
        fn = open('exist_album.txt', 'r')
        lines = fn.readlines()
        for line in lines:
            album_id = line.strip()
            exist_id.append(album_id)
        fn.close()
        for one in all_pages_list:
            if one['album_id'] not in exist_id:
                processed_list.append(one)
            else:
                pass
        return processed_list

    def get_insert_data(self, processed_list):
        '''
        :param processed_list:processed list which will be converted into "tuple list" that to be inserted into mysql
        :return:insert list
        '''
        insert_list = []
        for one in processed_list:
            time_var = one['time_var']
            album_id = one['album_id']
            album_title = one['album_title']
            album_href = one['album_href']
            album_name = one['album_name']
            author_href = one['author_href']
            author_text = one['author_text']
            insert_timestamp = str(int(time.time()))
            insert_list.append(
                (insert_timestamp, time_var, album_id, album_title, album_href, album_name, author_href, author_text))
        return insert_list

    def insert_into_mysql(self, insert_list, processed_list):
        sql = 'INSERT INTO baidu_albums(insert_timestamp,time_var,album_id,album_title,album_href,album_name,author_href,author_text) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
        try:
            self.cursor.executemany(sql, insert_list)
            self.database.commit()
            self.cursor.close()
            self.database.close()
            fn = open('exist_album.txt', 'a')
            for one in processed_list:
                t = one['album_id']
                fn.write(t)
                fn.write('\n')
                fn.flush()
            fn.close()
            fx = open('work_log.log', 'a')
            fx.write(str(time.time()) + '  ' + '插入了%d条' % len(processed_list))
            fx.write('\n')
            fx.flush()
            fx.close()
        except Exception, e:
            self.database.rollback()
            fn = open('error.log', 'a')
            fn.write(str(e))
            fn.write('\n')
            fn.flush()
            fn.close()

    def interface(self):
        urls = self.get_urls()
        processed_list = self.get_processed_list(urls)
        if len(processed_list) > 0:
            insert_list = self.get_insert_data(processed_list)
            self.insert_into_mysql(insert_list, processed_list)
        else:
            fx = open('work_log.log', 'a')
            fx.write(str(time.time()) + '  ' + '插入了0条')
            fx.write('\n')
            fx.flush()
            fx.close()

    def controller(self):
        while True:
            self.interface()
            time.sleep(86400)


if __name__ == '__main__':
    BaiduAlbums().controller()

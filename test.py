# coding=utf-8
import json
import urllib2
import re

import chardet
from bs4 import BeautifulSoup


# 通过视频的html页面获取视频播放地址
def parse_by_url(content):
    reg = r'http://mov.bn.netease.com/.*.m3u8'
    my = re.compile(reg, re.S)
    lists = re.findall(my, content)
    if len(lists) > 0:
        url = lists[0]
        if '-list.m3u8' in url:
            url = url.replace('-list.m3u8', '.flv')
        else:
            url = url.replace('.m3u8', '.mp4')
        return url


# 通过视频的html页面获取视频播放地址
def parse_by_url2(content):
    reg = r'http://swf.*.swf'
    my = re.compile(reg, re.S)
    lists = re.findall(my, content)
    if len(lists) > 0:
        url = lists[0]
        if '-list.m3u8' in url:
            url = url.replace('-list.m3u8', '.flv')
        else:
            url = url.replace('.m3u8', '.mp4')
        return url


def get_content(url):
    content = urllib2.urlopen(url, timeout=5).read()
    # content = unicode(content, "gb2312").encode("utf8")
    return content


def get_url(content):
    urlList2 = []
    soup = BeautifulSoup(content, 'html.parser', from_encoding='utf-8')
    courses = soup.find_all("a", target="_self")
    for course in courses:
        url = course['href']
        if url.startswith('http'):
            urlList2.append(url)
    return urlList2


def get_title(content):
    soup = BeautifulSoup(content, 'html.parser', from_encoding='UTF-8')
    title = soup.find_all("span", class_="f-fl f-thide sname")
    return title[0].string


def test(content):
    pattern = re.compile('.*title:"(.*?)"')
    result = re.search(pattern, content)
    print result


if __name__ == "__main__":
    url = "http://open.163.com/movie/2017/5/S/S/MCJ2N3HR0_MCMCCI6SS.html"
    content = get_content(url)
    u = parse_by_url2(content)
    print u

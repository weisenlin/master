# -*- coding: utf-8 -*-
import json
import urllib2

from bs4 import BeautifulSoup


# 通过首页的内容获取每个视频的详情页url
def get_detail(url):
    courseUrl = []
    # 国家图书馆公开课
    categories = {
        1, 2, 4, 5, 6, 7, 8, 9, 24,
        11, 13, 14, 15, 16, 25, 26
    }
    for category in categories:
        i = 1
        while i <= 10:
            url = url + "&page=" + str(i) + "&labelid=" + str(category)
            try:
                content = urllib2.urlopen(url, timeout=10).read()
            except:
                continue
            soup = BeautifulSoup(content, 'html.parser', from_encoding='utf-8')
            courses = soup.find_all("a", class_="toplay")
            count = 0
            for course in courses:
                href = 'http://open.nlc.cn/' + course['href']
                if href in courseUrl:
                    continue
                if count <= len(courses):
                    try:
                        cover = get_image(content)[count]['src']
                        detail = get_course_detail(content)[count]['href']
                        count = count + 1
                        print 'count:'+str(count)
                    except:
                        break
                    data = {}
                    data['url'] = href
                    data['cover'] = cover
                    data['category'] = category
                    # 详情页面
                    data['detail'] = "http://open.nlc.cn" + detail
                    try:
                        videos = find_video_list_dict(data['detail'])
                    except:
                        continue
                    data['videos'] = videos
                    courseUrl.append(data)
                    print data
            i = i + 1
    return courseUrl


# 将字典写入json文件的方法
def writeToJson(videos, str):
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    import codecs
    json.dump(videos, codecs.open(str, 'w', 'utf-8'), indent=4, ensure_ascii=False)


def get_content(url):
    content = urllib2.urlopen(url, timeout=5).read()
    return content


def get_real_content(content):
    soup = BeautifulSoup(content, 'html.parser', from_encoding='utf-8')
    cover = soup.find_all('div', class_="courses")
    soup = BeautifulSoup(str(cover), 'html.parser', from_encoding='utf-8')
    content = soup.find_all('ul')
    return content


# 获取课程封面
def get_image(content):
    content = get_real_content(content)
    soup = BeautifulSoup(str(content), 'html.parser', from_encoding='utf-8')
    cover = soup.find_all('img')
    return cover


# 获取课程详情页面地址
def get_course_detail(content):
    content = get_real_content(content)
    soup = BeautifulSoup(str(content), 'html.parser', from_encoding='utf-8')
    url = soup.find_all('a', target="_blank")
    return url


# 获取详情页信息
def get_info(url):
    data = {}
    content = urllib2.urlopen(url, timeout=10).read()
    soup = BeautifulSoup(content, 'html.parser', from_encoding='utf-8')
    target = soup.find_all('div', class_="player-box")
    title = soup.find_all('a', class_='title')[0].string
    description = soup.find_all('p', class_='content_font')[-1].string
    data['kvideoid'] = target[0]['data-kvideoid']
    data['murl'] = target[0]['data-murl']
    data['title'] = title
    data['description'] = description
    return data


# 输出课程信息
def insert_course():
    url = "http://open.nlc.cn/kvideo.php?do=search"
    courseUrl = get_detail(url)
    writeToJson(courseUrl, 'courses.json')


# 输出单曲信息  还没有完成
def insert_video_url():
    url = "http://open.nlc.cn/kvideo.php?do=search"
    courseUrl = get_detail(url)
    for course in courseUrl:
        url = course['detail']
        find_course_list(url)


# 打印详情页信息
def printInfo(info):
    print "kvideoid:"+info[0], "url:"+info[1], "title:"+info[2], "description:"+info[3]


# 查找课程内目录地址
def find_course_list(url):
    content = get_content(url)
    soup = BeautifulSoup(content, 'html.parser', from_encoding='utf-8')
    content = soup.find_all('div', class_="series-nav1")
    soup = BeautifulSoup(str(content), 'html.parser', from_encoding='utf-8')
    content = soup.find_all('li')[1]
    soup = BeautifulSoup(str(content), 'html.parser', from_encoding='utf-8')
    content = soup.find_all('a')
    return "http://open.nlc.cn" + content[0]['href']


# 根据课程详情页url查找课程内所有视频地址
def find_video_list(url):
    videourl = find_course_list(url)
    content = get_content(videourl)
    soup = BeautifulSoup(content, 'html.parser', from_encoding='utf-8')
    content = soup.find_all('div', class_='directory')
    title = soup.find_all('div', class_="directoryName")
    soup = BeautifulSoup(str(content), 'html.parser', from_encoding='utf-8')
    content = soup.find_all('ul')
    soup = BeautifulSoup(str(content), 'html.parser', from_encoding='utf-8')
    content = soup.find_all('a')
    count = len(content)
    urls = []
    for i in range(count):
        data = {}
        data['url'] = "http://open.nlc.cn" + content[i]['href']
        data['title'] = title[i].string
        urls.append(data)
    return urls


#

# 根据课程详情页url查找课程内所有视频地址存储为字典
def find_video_list_dict(url):
    videourl = find_course_list(url)
    content = get_content(videourl)
    soup = BeautifulSoup(content, 'html.parser', from_encoding='utf-8')
    content = soup.find_all('div', class_='directory')
    title = soup.find_all('div', class_="directoryName")
    soup = BeautifulSoup(str(content), 'html.parser', from_encoding='utf-8')
    content = soup.find_all('ul')
    soup = BeautifulSoup(str(content), 'html.parser', from_encoding='utf-8')
    content = soup.find_all('a')
    count = len(content)
    urls = []
    for i in range(count):
        data = {}
        data['videourl'] = "http://open.nlc.cn" + content[i]['href']
        data['videotitle'] = title[i].string
        data['videoinfo'] = get_info(data['videourl'])
        urls.append(data)
    return urls


if __name__ == '__main__':
    insert_course()
    # url = "http://open.nlc.cn/mooc/9420"
    # content = find_video_list_dict(url)
    # for c in content:
    #     print c['videoinfo']['title']





#!/usr/bin/python3
#coding: utf8

import os, sys, time, datetime
import re, json, copy, logging
import signal

PY35 = sys.version_info.major == 3 and sys.version_info.minor >= 5
PY2 = sys.version_info.major == 2

if not PY35:
    if PY2:
        reload(sys)
        sys.setdefaultencoding('utf-8')
    raise Exception('请使用 Python3.5 以上的版本！')

from functools    import partial, reduce

import concurrent.futures

"""
    $ pip3 install bs4 requests
    $ python3 jianshu.py
"""
import requests
import bs4


USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'

BASE_HEADERS = {
    'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4',
    'Host': 'www.jianshu.com',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'X-Requested-With': 'XMLHttpRequest',
    'Accept': 'text/html, */*; q=0.01',
    'User-Agent': USER_AGENT,
    'Connection': 'keep-alive',
    'Referer': 'http://www.jianshu.com',
}

AJAX_HEADERS = copy.deepcopy(BASE_HEADERS)
AJAX_HEADERS['X-PJAX'] = 'true'

JIANSHU_USER_STRUCT = {
    "slug": None,
    "nickname": None,
    "is_contract": None,
    "description": None,
    "total_following_count": None,
    "total_followers_count": None,
    "total_articles_count": None,
    "total_wordage": None,
    "total_likes_count": None,
    "contact": None,

    "following": [],
    "followers": []
}

HTML_PARSER  = 'html.parser'
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR    = os.path.join(PROJECT_ROOT, 'cache/jianshu')

thread_pool  = concurrent.futures.ThreadPoolExecutor(max_workers=400)
logger       = logging.getLogger('crawler.jianshu')

def fetch_recommended_users(*args, **kwargs):
    recommended_users = []
    page = 1
    while 1:
        url   = "http://www.jianshu.com/users/recommended?page=%d&per_page=200" % page
        users = []

        logger.info("GET %s ...", url)
        
        try:
            r = requests.get(url, headers=BASE_HEADERS)
        except:
            break

        if r.status_code != 200:
            break
        try:
            data = json.loads(r.text)
            assert('users' in data)
            assert(type(data['users']) == list)
            for user in data['users']:
                if 'K' in user['total_wordage']:
                    user['total_wordage'] = float(user['total_wordage'].replace("K", ''))*1000
                if 'K' in user['total_likes_count']:
                    user['total_likes_count'] = float(user['total_likes_count'].replace("K", ''))*1000
                user['contact'] = None
            users = data['users']
        except Exception as e:
            logger.exception(e)
            break

        if users is None:
            break
        if len(users) == 0:
            break
        recommended_users.extend(users)
        page += 1

    return list(map(lambda u: u['slug'], recommended_users))


def pull(user_slug):
    filename = os.path.join(CACHE_DIR, user_slug+".json")
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    
    user = copy.deepcopy(JIANSHU_USER_STRUCT)
    user['slug'] = user_slug

    def detail():
        url = "http://www.jianshu.com/u/%s" % user_slug
        logger.info("GET %s", url)
        table = (
            ('关注', 'total_following_count'), ('粉丝', 'total_followers_count'),
            ('文章', 'total_articles_count'), ('字数', 'total_wordage'), ('收获喜欢', 'total_likes_count')
        )
        try:
            r = requests.get(url, headers=BASE_HEADERS)
            soup = bs4.BeautifulSoup(r.text, HTML_PARSER)
            main_top_elem = soup.find('div', class_="main-top")
            if main_top_elem:
                elems = list(map(lambda elem: elem.text.strip(), main_top_elem.find_all("div", class_="meta-block") ))

                for elem in elems:
                    for kw, key in table:
                        if kw in elem:
                            tmp = re.compile(r"\d+", re.DOTALL).findall(elem)
                            if len(tmp) > 0:
                                user[key] = int(tmp[0])
                
                author_tag_elem = main_top_elem.find('span', class_="author-tag")
                if author_tag_elem:
                    user['is_contract'] = True
                else:
                    user['is_contract'] = False
                name_elem = main_top_elem.find("a", class_="name")
                if name_elem:
                    user['nickname'] = name_elem.text.strip()
                
                description_elem = soup.find("div", class_="description")
                if description_elem:
                    bio_elem = description_elem.find("div", class_="js-intro")
                    if bio_elem:
                        user['description'] = bio_elem.prettify().strip()
                    social_elems = description_elem.find_all("a", class_="social-icon-sprite")
                    
                    contact = {}
                    for social_elem in social_elems:
                        _tmp = list(filter(lambda name: name != 'social-icon-sprite', social_elem.attrs.get('class')))
                        extra_class_name = 'unknow'
                        if len(_tmp) > 0:
                            extra_class_name = _tmp[0].replace("social-icon-", "")

                        href_attr = social_elem.attrs.get('href')
                        data_content_attr = social_elem.attrs.get('data-content')
                        if data_content_attr is not None and data_content_attr != 'javascript:void(0);':
                            contact[extra_class_name] = data_content_attr
                        elif href_attr is not None and href_attr != 'javascript:void(0);':
                            contact[extra_class_name] = href_attr
                        else:
                            pass
                    user['contact'] = contact

        except Exception as e:
            logger.exception(e)

        
    def parse(html):
        soup = bs4.BeautifulSoup(html, HTML_PARSER)
        elems = soup.find_all("a", class_="name")
        slugs = list(map(lambda elem: elem.attrs.get('href').replace("/u/", ""), elems))
        return slugs

    def following(page=1):
        url = "http://www.jianshu.com/users/%s/following?page=%d" % (user_slug, page)
        
        logger.info("GET %s", url)

        elems = []
        try:
            r = requests.get(url, headers=AJAX_HEADERS)
            elems = parse(r.text)
        except:
            pass
        
        if len(elems) == 0:
            return elems
        else:
            elems.extend(following(page=page+1))
            return elems

    def followers(page=1):
        url = "http://www.jianshu.com/users/%s/followers?page=%d" % (user_slug, page)
        
        logger.info("GET %s", url)

        elems = []
        try:
            r = requests.get(url, headers=AJAX_HEADERS)
            elems = parse(r.text)
        except:
            pass

        if len(elems) == 0:
            return elems
        else:
            elems.extend(followers(page=page+1))
            return elems

    if not os.path.exists(filename):
        detail()
        user['following'] = following()
        user['followers'] = followers()

        bad = False
        fileds = (
            'slug', 'nickname', 'is_contract', 'description', 'total_following_count', 
            'total_followers_count', 'total_articles_count', 'total_wordage', 'total_likes_count'
        )
        for k in fileds:
            if user[k] is None:
                bad = True

        if not bad:
            logger.info("write %s", filename)
            open(filename, "wb").write(json.dumps(user).encode("utf8"))
    else:
        try:
            data = json.loads(open(filename, "rb").read().decode("utf8"))
            assert('following' in data and type(data['following']) == list)
            assert('followers' in data and type(data['followers']) == list)

            user['following'] = data['following']
            user['followers'] = data['followers']
        except Exception as e:
            logger.exception(e)

    for slug in user['following']:
        thread_pool.submit(partial(pull, slug))

    for slug in user['followers']:
        thread_pool.submit(partial(pull, slug))


def main():
    for slug in fetch_recommended_users():
        thread_pool.submit(partial(pull, slug))

    shutdown  = False
    last_time = None
    while not shutdown:
        if thread_pool._work_queue.unfinished_tasks == 0:
            if last_time is None:
                last_time = time.time()
            else:
                now_time = time.time()
                if now_time - last_time > 120:
                    shutdown = True
        else:
            last_time = None

        logger.info("unfinished_tasks: %d\tqsize: %d", thread_pool._work_queue.unfinished_tasks, thread_pool._work_queue.qsize())
        time.sleep(5)

    logger.info("DONE!")


if __name__ == '__main__':
    logging.basicConfig(
        format  = '%(asctime)s %(levelname)-5s %(threadName)-10s %(name)-15s %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S',
        level   = logging.DEBUG
    )
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    signal.signal(signal.SIGINT,  signal.SIG_DFL)
    signal.signal(signal.SIGSEGV, signal.SIG_DFL)
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    main()

import requests
from bs4 import BeautifulSoup as bs
from datetime import date
from dateutil.rrule import rrule, DAILY
import re
from multiprocessing.dummy import Pool as ThreadPool

def clear(text):
    return re.sub( r'\s+', ' ', text)

def split(arr, count):
    '''Делим ссылки на части для мультипотока'''
    return [arr[i::count] for i in range(count)]

def all_url():
    '''генерируем ссылки за указанный период
    Поменяйте в ссылке регион'''
    a = date(2019, 1, 1) #Начало периода
    b = date(2019, 4, 10) #конец периода
    url_all = []
    for dt in rrule(DAILY, dtstart=a, until=b):
        date_select = (dt.strftime("%d.%m.%Y"))
        url = 'http://www.zakupki.gov.ru/epz/order/extendedsearch/results.html?' \
              'morphology=on&openMode=USE_DEFAULT_PARAMS&pageNumber=1&' \
              'sortDirection=false&recordsPerPage=_100&showLotsInfoHidden=' \
              'false&fz44=on&fz223=on&ppRf615=on&fz94=on&af=on&ca=on&pc=on&pa=' \
              'on&currencyIdGeneral=-1&publishDateFrom={}&publishDateTo={}&region_regions_5277386=' \
              'region_regions_5277386&regions=5277386&regionDeleted=' \
              'false&sortBy=UPDATE_DATE'.format(date_select,date_select)
        url_all.append(url)
    return url_all

def get_page(uri):
    '''Парсинг страницы'''
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) '
                             'AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/71.0.3578.98 Safari/537.36'}
    response = requests.get(uri, headers=headers ).text
    soup = bs(response, 'html.parser')
    tag = soup.findAll( "div", {"class": "registerBox"} )
    zakup_all = []
    for i in tag:
        try:
            one_columne = i.find('td')
            text_type = one_columne.find('strong').text
            text_fz = one_columne.find('span', {"class": "orange"}).text
            text_rub = (one_columne.find( 'span', {"class": "fractionNumber"}).parent).text
            text_rub = re.sub(r' ', '', clear(text_rub))

            two_columne = i.find('td', {"class": "descriptTenderTd"})
            text_num = two_columne.find('dt').text
            text_num = (clear(text_num))
            text_num_url = two_columne.find('dt')
            text_num_url = 'http://www.zakupki.gov.ru' + text_num_url.find("a").get('href')
            text_org = two_columne.find('li').text
            text_org_url = 'http://www.zakupki.gov.ru'+(two_columne.find('li').find("a").get('href'))
            text_main = two_columne.findAll( 'dd' )[1].text

            tree_columne = i.find('td', {"class": "amountTenderTd"})
            text_date = (tree_columne.findAll('li')[0].text)[11:]
            text_update = (tree_columne.findAll( 'li' )[1].text)[11:]

            text_type = (clear(text_type))
            text_org = (clear(text_org))
            text_main = (clear(text_main))

            zakup = (text_type, text_fz,text_rub, text_num, text_org,
                     text_main, text_date, text_update, text_num_url)
            zakup_all.append(zakup)
        except:
            z=0
    return zakup_all

'''Парсим мультипотоком и сохраняем'''
url_count = len(all_url())
pagi = round(url_count / 5)
pool = ThreadPool(5)
page_count = []

record_file = 'zakup.csv'
with open(record_file, 'a') as f:
    for part in (split(all_url(), pagi)): #458
        page_count.append(len(part))
        print( 'Страниц обработано: {} из {}'.format(sum( page_count ), url_count))
        try:
            date_array = pool.map(get_page, part)
            for sep in date_array:
                if sep == True:
                    for item in sep:
                        stroka = '|'.join(item)
                        f.write(stroka + '\n')
        except IOError as e:
            print("Ошибка записи в файл")
            print(e)
print ('Парсинг данных завершён успешно.')

"""
Многопоточный парсер госзакупок с сохранением в csv
https://github.com/tarbagan/zakup2
Автор: Иргит Валерий
Версия 2
"""
from multiprocessing.dummy import Pool as ThreadPool
from dateutil.rrule import rrule, DAILY
from bs4 import BeautifulSoup as bs
from datetime import date
import requests
import re

a = date(2020, 1, 1)  # Начало периода
b = date(2020, 4, 19)  # конец периода
id_region = '5277386'  # id региона
thr = 5  # кол-во потоков
record_file = 'zakup.csv'


def split(arr, thr):
    """Делим ссылки на части для мультипотока"""
    return [arr[i::thr] for i in range(thr)]


def gen_url():
    """Генерация ссылок по дате"""
    start_url = []
    for dt in rrule(DAILY, dtstart=a, until=b):
        date = dt.strftime("%d.%m.%Y")
        url = 'https://zakupki.gov.ru/epz/order/extendedsearch/results.html?searchString=&morphology=on&' \
              'search-filter=%D0%94%D0%B0%D1%82%D0%B5+%D1%80%D0%B0%D0%B7%D0%BC%D0%B5%D1%89%D0%B5%D0%BD%D0%B8%D1%8F&pageNumber=1&' \
              'sortDirection=false&recordsPerPage=_100&showLotsInfoHidden=false&savedSearchSettingsIdHidden=&' \
              'sortBy=UPDATE_DATE&fz44=on&fz223=on&ppRf615=on&fz94=on&af=on&ca=on&pc=on&pa=on&placingWayList=&' \
              'okpd2Ids=&okpd2IdsCodes=&selectedSubjectsIdHidden=&npaHidden=&restrictionsToPurchase44=&' \
              'publishDateFrom={}&publishDateTo={}&applSubmissionCloseDateFrom=&' \
              'applSubmissionCloseDateTo=&priceFromGeneral=&priceFromGWS=&priceFromUnitGWS=&priceToGeneral=&' \
              'priceToGWS=&priceToUnitGWS=&currencyIdGeneral=-1&customerIdOrg=&agencyIdOrg=&' \
              'customerPlace=5277386&customerPlaceCodes=17000000000&OrderPlacementSmallBusinessSubject=on&' \
              'OrderPlacementRnpData=on&OrderPlacementExecutionRequirement=on&orderPlacement94_0=0&' \
              'orderPlacement94_1=0&orderPlacement94_2=0'.format(date, date)
        start_url.append(url)
    return start_url


def request_url(url):
    """Получаем страницу"""
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) '
                             'AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/71.0.3578.98 Safari/537.36'}

    try:
        r = requests.get(url, headers=headers).text
        soup = bs(r, 'html.parser')
        return soup
    except Exception as e:
        print(e)


def clear(text):
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r' ₽', '', text)
    text = text.lstrip()
    text = text.rstrip()
    return text


def parser_start(soup):
    arr_item = []
    block = soup.findAll('div', {'class': 'search-registry-entry-block'})
    try:
        for item in block:
            text_fz = item.find('div', {'class': 'registry-entry__header-top__title text-truncate'}).text
            text_fz = clear(text_fz)  # Тип аукциона

            text_nm = item.find('div', {'class': 'registry-entry__header-mid__number'}).text
            text_nm = clear(text_nm)  # Код аукциона

            text_ur = item.find('div', {'class': 'registry-entry__header-mid__number'}).findAll('a')[0].get(
                'href')  # Ссылка

            text_st = item.find('div', {'class': 'registry-entry__header-mid__title'}).text
            text_st = clear(text_st)  # Статус

            text_pr = item.find('div', {'class': 'price-block__value'}).text
            text_pr = clear(text_pr)  # Статус

            text_dt = item.find('div', {'class': 'data-block__value'}).text  # дата

            text_ds = item.find('div', {'class': 'registry-entry__body-value'}).text
            text_ds = clear(text_ds)  # Описание

            text_zk = item.find('div', {'class': 'registry-entry__body-href'}).text
            text_zk = clear(text_zk)  # Заказчик

            data_item = (text_fz, text_nm, text_ur, text_st, text_pr, text_dt, text_ds, text_zk)
            arr_item.append(data_item)
    except Exception as e:
        print(e)
    return arr_item



pool = ThreadPool(thr)
page_count = []
item_count = []

with open(record_file, 'a') as f:
    for arr_url in split(gen_url(), thr):
        date_array = pool.map(request_url, arr_url)
        for page in date_array:
            page_count.append(page)
            data = parser_start(page)
            if data:
                for item in data:
                    dat = '|'.join(item)
                    f.write(dat + '\n')

                    # логирование
                    item_count.append(1)
                    p_count = len(page_count)
                    z_count = len(item_count)
                    print(f'Страница {p_count}/закупок {z_count}: {item[1]}')

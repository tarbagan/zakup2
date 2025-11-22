# -*- coding: utf-8 -*-
"""
Многопоточный парсер госзакупок с сохранением в csv
https://github.com/tarbagan/zakup2
Автор: Иргит Валерий
Версия: 0.3 (исправленная)
"""

from multiprocessing.dummy import Pool as ThreadPool
from dateutil.rrule import rrule, DAILY
from bs4 import BeautifulSoup as bs
from datetime import date
import requests
import re
import csv
import time

# -------------------------
# НАСТРОЙКИ
# -------------------------
a = date(2013, 1, 1)        # Начало периода
b = date(2025, 11, 22)       # Конец периода (включительно)
id_region = '17000000000'       # id региона (customerPlace)
thr = 5                     # Кол-во потоков
record_file = r'g:\WORK\Парсер закупок\zakup2.csv'  # Файл с результатами


def gen_urls(start_date, end_date, region_id):
    """Генерация ссылок по датам"""
    urls = []
    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        date_str = dt.strftime("%d.%m.%Y")
        url = (
            "https://zakupki.gov.ru/epz/order/extendedsearch/results.html?"
            "searchString=&morphology=on&"
            "search-filter=%D0%94%D0%B0%D1%82%D0%B5+%D1%80%D0%B0%D0%B7%D0%BC%D0%B5%D1%89%D0%B5%D0%BD%D0%B8%D1%8F&"
            "pageNumber=1&"
            "sortDirection=false&recordsPerPage=_100&showLotsInfoHidden=false&savedSearchSettingsIdHidden=&"
            "sortBy=UPDATE_DATE&fz44=on&fz223=on&ppRf615=on&fz94=on&af=on&ca=on&pc=on&pa=on&placingWayList=&"
            "okpd2Ids=&okpd2IdsCodes=&selectedSubjectsIdHidden=&npaHidden=&restrictionsToPurchase44=&"
            "publishDateFrom={d}&publishDateTo={d}&applSubmissionCloseDateFrom=&"
            "applSubmissionCloseDateTo=&priceFromGeneral=&priceFromGWS=&priceFromUnitGWS=&priceToGeneral=&"
            "priceToGWS=&priceToUnitGWS=&currencyIdGeneral=-1&customerIdOrg=&agencyIdOrg=&"
            "customerPlace={region}&customerPlaceCodes={region}&OrderPlacementSmallBusinessSubject=on&"
            "OrderPlacementRnpData=on&OrderPlacementExecutionRequirement=on&orderPlacement94_0=0&"
            "orderPlacement94_1=0&orderPlacement94_2=0"
        ).format(d=date_str, region=region_id)
        urls.append(url)
    return urls


def request_url(url, retries=3, timeout=15):
    """Получаем страницу, с повторными попытками при ошибке"""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/71.0.3578.98 Safari/537.36"
        )
    }

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            # На всякий случай корректируем кодировку
            if not resp.encoding:
                resp.encoding = resp.apparent_encoding
            html = resp.text
            soup = bs(html, "html.parser")
            return soup
        except Exception as e:
            print(f"[WARN] Ошибка при запросе {url} (попытка {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(2)
    # Если все попытки неудачны, вернем None
    return None


def clear(text):
    """Очистка текста"""
    if text is None:
        return ""
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r" ₽", "", text)
    text = text.strip()
    return text


def parser_start(soup):
    """Парсер данных со страницы результатов"""
    items = []

    if soup is None:
        return items

    blocks = soup.find_all("div", {"class": "search-registry-entry-block"})
    if not blocks:
        return items

    for block in blocks:
        try:
            text_fz = block.find(
                "div", {"class": "registry-entry__header-top__title text-truncate"}
            )
            text_fz = clear(text_fz.get_text() if text_fz else "")

            num_div = block.find("div", {"class": "registry-entry__header-mid__number"})
            text_nm = clear(num_div.get_text() if num_div else "")

            # Ссылка
            link_tag = num_div.find("a") if num_div else None
            text_ur = link_tag.get("href") if link_tag else ""

            # Статус
            st_div = block.find("div", {"class": "registry-entry__header-mid__title"})
            text_st = clear(st_div.get_text() if st_div else "")

            # Цена
            pr_div = block.find("div", {"class": "price-block__value"})
            text_pr = clear(pr_div.get_text() if pr_div else "")

            # Дата
            dt_div = block.find("div", {"class": "data-block__value"})
            text_dt = clear(dt_div.get_text() if dt_div else "")

            # Описание
            ds_div = block.find("div", {"class": "registry-entry__body-value"})
            text_ds = clear(ds_div.get_text() if ds_div else "")

            # Заказчик
            zk_div = block.find("div", {"class": "registry-entry__body-href"})
            text_zk = clear(zk_div.get_text() if zk_div else "")

            items.append((text_fz, text_nm, text_ur, text_st, text_pr, text_dt, text_ds, text_zk))
        except Exception as e:
            # Пропускаем проблемный блок, не валим весь парсинг
            print(f"[WARN] Ошибка при разборе блока: {e}")
            continue

    return items


def main():
    print("Генерация списка URL...")
    urls = gen_urls(a, b, id_region)

    total_pages = len(urls)
    print(f"Всего страниц для обработки: {total_pages}")

    page_counter = 0
    item_counter = 0

    print("Начинаю сбор данных, подождите...")

    with open(record_file, "w", encoding="utf-8", newline="") as f, \
            ThreadPool(thr) as pool:

        writer = csv.writer(f, delimiter="|")
        # Заголовки – можно изменить под вашу схему
        writer.writerow(
            ["fz_type", "number", "url", "status", "price", "publish_date", "description", "customer"]
        )

        # Идём по страницам в произвольном порядке, чтобы не ждать «медленные» даты
        for soup in pool.imap_unordered(request_url, urls):
            page_counter += 1

            if soup is None:
                print(f"[INFO] Страница {page_counter}/{total_pages} пропущена (ошибка запроса)")
                continue

            data = parser_start(soup)
            if not data:
                print(f"[INFO] На странице {page_counter}/{total_pages} закупок не найдено")
                continue

            for row in data:
                writer.writerow(row)
                item_counter += 1
                # Можно включить более подробный лог, но это замедлит парсер:
                # print(f"Страница {page_counter}/{total_pages}, закупка: {row[1]}")

            print(f"[OK] Обработана страница {page_counter}/{total_pages}, всего закупок: {item_counter}")

    print(f"\nСбор данных завершён. Результат записан в файл '{record_file}'.")
    print(f"Всего обработано страниц: {page_counter}")
    print(f"Всего найдено закупок: {item_counter}")


if __name__ == "__main__":
    main()

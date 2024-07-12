url = 'https://www.jd.com/chanpin/25877.html'

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cookie': 'pinId=iuXMAaF7pVkB-ZaUy57Ew7V9-x-f3wj7; shshshfpa=3f810591-326a-a787-d906-3e3f78baaea0-1657332207; shshshfpx=3f810591-326a-a787-d906-3e3f78baaea0-1657332207; pin=jd_4a800764b8169; unick=%E5%B8%85%E6%B0%94%E7%9A%84%E5%B0%8F%E4%B9%8C%E9%BE%9F11; _tp=A10MK3vojkhCxe%2F%2FR6NZNahNbdoQZNVWIJpZqHFSRcE%3D; _pst=jd_4a800764b8169; __jdu=1718851363242970084230; TrackID=1vZnuwwtov_A15EyBX1Ptq6KsgHLQjsvXkP2K9VjXkknB1MSE_W3BfYuDEAsHK_M0K1M7XM1C5pKaDS6OWo_PNVCjmtdENTj5B0RmOB1yfPw; thor=3126B90478C57763EFC4D4DE5D592AA0D7091767C9D65192EA5F7B1336BDC11B3C5BACE47E73A11404E817A68AD2BBBAAFD8D65E955AAD28190D58F3C8546FD079E3AF0554BECED353BC3FBB12A0277726CA229A15DD031BB5D87D0112E2BF6210A55ECD8F87A32794656E5EE54CEDB1C8D719A4964AAAD44F1D4E75346C2BE1F68AB2005BEA105EC5AC826768933F9872A8DC77CA5429E4CBCA204F764AFA56; areaId=19; o2State={%22webp%22:true%2C%22avif%22:true%2C%22lastvisit%22:1718851383251}; UseCorpPin=jd_4a800764b8169; unpl=JF8EALJnNSttCktQUUlSExAQT19VW1oMTh8CPDRWUw8NTVRWSAVPFUJ7XlVdWBRKEB9sZhRUXFNPVQ4YBysSEXteU11bD00VB2xXXAQDGhUQR09SWEBJJV1UW1kATxIHbW4AZG1bS2QFGjIbFBBCVFBeXw9JFAZsbwNVXFFOVwcaMhoiF3ttZFhZDkITBF9mNVVtGh8IDR0FHBAVBl1SXlQBTxcBaGUGUV5QTVUEEgcYEBF7XGRd; __jdv=76161171|haosou-search|t_262767352_haosousearch|cpc|11459545384_0_c15eca931530465490bbb6cd71bb6e6c|1718851824725; user-key=2abe7148-6931-4ee6-b2e0-0693affceb9c; cn=4; areaId=22; ipLoc-djd=22-2103-2105-0; jsavif=1; flash=2_Ii7PLhREZVpWWKUq5GTrYCfhndnnubphC1EeoZwY4wHyA6_bR9-8F-ZhPvngAXB-tnUS-DXYwh4F9V_VcT-N-zHz4Q1TKOeA3XhNLRwSDbGP1_AXt32bzr9mPzrUnAALLqdqW0iZLm_kP_nSv8-SkuIgOnjvhiDFeJR6GgDhYHe*; ipLoc-djd=19-1611-19920-19972; shshshfpb=BApXcVWWhMPVAd6uBwC1JM2vPV0jDfB96BxVyJSgU9xJ1Mv6sboC2; __jda=76161171.1718851363242970084230.1718851363.1718851363.1718851825.2; __jdc=76161171; 3AB9D23F7A4B3CSS=jdd03QR5ZHLZ7W3P4RYMUU37F5KT74EJG72GAFTLN3KJJDRHRAU46NUODBSYTO7J2T6V4HWGTFLHTBLRDAELSMEOS2JSECUAAAAMQGOV4SYIAAAAACT3UAJJQNCPXFUX; _gia_d=1; __jdb=76161171.29.1718851363242970084230|2.1718851825; 3AB9D23F7A4B3C9B=QR5ZHLZ7W3P4RYMUU37F5KT74EJG72GAFTLN3KJJDRHRAU46NUODBSYTO7J2T6V4HWGTFLHTBLRDAELSMEOS2JSECU',
    'Priority': 'u=0, i',
    'Referer': 'https://www.jd.com/chanpin/26134.html',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}

import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

# set driver
service = Service(r'D:\OneDrive - 东南大学\5 我的代码\crawling\chromedriver\chromedriver.exe')
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
prefs = {"profile.managed_default_content_settings.images": 2, 'permissions.default.stylesheet': 2}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument('lang=zh_CN.utf-8')

driver = webdriver.Chrome(service=service, options=chrome_options)

# get url
driver.get(url)
wait = WebDriverWait(driver, 10)
time.sleep(1)

datas = []

# iterate page
total_items = driver.find_element(By.CSS_SELECTOR, '#J_resCount').text
print('total items: %s' % total_items)

page = 1
g_id = 1

while True:

    if g_id >= int(total_items):
        break

    try:
        # read data
        goodlist = driver.find_element(By.CSS_SELECTOR, '#J_goodsList > ul')
        goodlist = goodlist.find_elements(By.CLASS_NAME, 'gl-item')
        print('get good list')

        for good in goodlist:
            # basic info
            sku = good.get_attribute("data-sku")
            spu = good.get_attribute("data-spu")
            product_name = good.find_element(By.CLASS_NAME, "p-name").text
            product_link = good.find_element(By.CLASS_NAME, 'p-img').find_element(By.TAG_NAME, 'a').get_attribute('href')
            price = good.find_element(By.CLASS_NAME, "p-price").find_element(By.TAG_NAME, 'strong').get_attribute('data-price')
            commit_count = good.find_element(By.CLASS_NAME, "p-commit").text

            # save
            data = [sku, spu, product_name, product_link, price, commit_count]
            datas.append(data)
            print(g_id, data)
            g_id += 1

        # next page
        fp_next = driver.find_element(By.CSS_SELECTOR, '#J_topPage > a.fp-next')
        fp_next.send_keys(Keys.RIGHT)
        wait = WebDriverWait(driver, 10)
        print("page %d done" % page)
        page += 1
        time.sleep(1)
        
    except:
        print("Something went wrong...")
        break

# quit
driver.quit()

# save
print("=======================================")
print("saving...")
df = pd.DataFrame(datas, columns=['sku', 'spu', 'name', 'link', 'price', 'commit_count'])
df.to_excel('./jd/jd_products.xlsx', index=False)
print("done!")

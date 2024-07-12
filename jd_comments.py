import urllib.parse
import json
from requests_html import HTMLSession
import time
import pandas as pd

brake_pad = urllib.parse.quote('汽车刹车片')

front_brake = urllib.parse.quote('878_97057^')
rear_brake = urllib.parse.quote('878_97058^')

low_meta = urllib.parse.quote('3237_96774^')
semi_meta = urllib.parse.quote('3237_96775^')
ceramic = urllib.parse.quote('3237_6887^')
NAO = urllib.parse.quote('3237_59463^')

def get_url(kwd, tags=None, brand=None, page=1, sort=0):
    """sort params
    none(1): default
    psort=1: price desending
    psort=2: price assending
    psort=3: sale
    psort=4: comment number
    psort=5: date
    """
    # generate tags
    tag = ''.join(tags) if tags else ''

    # gengerate url
    if brand:
        format = 'https://search.jd.com/Search?keyword=%s&ev=%sexbrand_%s&cid3=11859&page=%d' % (kwd, tag, brand, page*2-1)
    else:
        format = 'https://search.jd.com/Search?keyword=%s&ev=%s&cid3=11859&page=%d' % (kwd, tag, page*2-1)
    if sort == 0:
        return format
    else:
        format += '&psort=%d' % sort
        return format
    
def get_brands():
    # get response
    with open(r'D:\OneDrive - 东南大学\5 我的代码\crawling\e-goods sale\brands.txt', 'rb') as f:
        brands = json.load(f)
    return brands


def get_response(url, headers):
    """
    Obtain html response.
    """
    session = HTMLSession()
    r = session.get(url, headers=headers)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    session.close()
    return r

def get_comment(id, headers):
    """
    Product id.
    """
    # get comment url
    comment_url = 'https://club.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv100&productId=%d&score=0&sortType=5&page=0&pageSize=10' % id
    comment = get_response(comment_url, headers=headers)
    comment_n = comment.text.split("\"commentCountStr\":\"")[1].split("\"")[0]
    comment_n = comment_n.strip("+")
    # 10 thousand
    if "万" in comment_n:
        comment_n = comment_n.split("万")[0]
        comment_n += '0000'
    return float(comment_n)

def extract_data(response, headers):
    """
    Extract data from good list.
    """
    data = []
    good_list = response.html.find('#J_goodsList > ul', first=True)
    goods = good_list.find('li')
    for good in goods:
        id = int(good.attrs['data-sku'])
        items = good.find('div')
        name = items[4].text
        price = items[3].text.strip('￥')
        time.sleep(0.5)
        comment_n = get_comment(id, headers)
        commit_link = items[5].absolute_links.pop()
        shop = items[6].text
        data.append([id, name, price, comment_n, commit_link, shop])
    return data

def transfer_df(data, brake_type, material, brand):
    """
    data: list
    brake type: front or rear
    """
    df = pd.DataFrame(data, columns=['id', 'name', 'price', 'comments', 'link', 'shop'])
    df['brake type'] = name_dict[brake_type]
    df['material'] = name_dict[material]
    df['brand'] = brand
    return df

name_dict = {
    front_brake: 'front',
    rear_brake: 'rear',
    low_meta: 'low metallic',
    semi_meta: 'semimetalic',
    ceramic: 'ceramic',
    NAO: 'NAO'
}

def merge_df(dfs):
    """
    dfs: list of df
    """
    df_concatenated = pd.concat(dfs)
    df_union = df_concatenated.drop_duplicates(keep=False)
    df_union.reset_index(drop=True, inplace=True)
    return df_union

def main(brake_type, material, brand, headers):
    """
    brake_type: front or rear
    """
    df = pd.DataFrame()

    for sort in [0,1,2,3,4,5]:  # iterate all sorting methods
        page = 1
        while True:  # search all pages
            # get url
            url = get_url(brake_pad, [brake_type, material], brand, page=page, sort=sort)
            
            try:
                print("==========================")
                print("Crawling at [%s] [%s] [%s]: sort [%d] page [%d]" % (brand, name_dict[brake_type], name_dict[material], sort, page))
                response = get_response(url, headers)
                time.sleep(0.1)
                data = extract_data(response, headers)
                df_ = transfer_df(data, brake_type, material, brand)
                df = merge_df([df, df_])
            except:  # no data for keywords or end of page
                print("No data for [%s] [%s] [%s]: sort [%d] page [%d]" % (brand, name_dict[brake_type], name_dict[material], sort, page))
                break
            
            page += 1
    return df

brake_type = rear_brake
material = semi_meta
headers = {
        'Cookie': 'pinId=iuXMAaF7pVkB-ZaUy57Ew7V9-x-f3wj7; shshshfpa=3f810591-326a-a787-d906-3e3f78baaea0-1657332207; shshshfpx=3f810591-326a-a787-d906-3e3f78baaea0-1657332207; pin=jd_4a800764b8169; unick=%E5%B8%85%E6%B0%94%E7%9A%84%E5%B0%8F%E4%B9%8C%E9%BE%9F11; _tp=A10MK3vojkhCxe%2F%2FR6NZNahNbdoQZNVWIJpZqHFSRcE%3D; _pst=jd_4a800764b8169; qrsc=3; __jdu=1718851363242970084230; TrackID=1vZnuwwtov_A15EyBX1Ptq6KsgHLQjsvXkP2K9VjXkknB1MSE_W3BfYuDEAsHK_M0K1M7XM1C5pKaDS6OWo_PNVCjmtdENTj5B0RmOB1yfPw; thor=3126B90478C57763EFC4D4DE5D592AA0D7091767C9D65192EA5F7B1336BDC11B3C5BACE47E73A11404E817A68AD2BBBAAFD8D65E955AAD28190D58F3C8546FD079E3AF0554BECED353BC3FBB12A0277726CA229A15DD031BB5D87D0112E2BF6210A55ECD8F87A32794656E5EE54CEDB1C8D719A4964AAAD44F1D4E75346C2BE1F68AB2005BEA105EC5AC826768933F9872A8DC77CA5429E4CBCA204F764AFA56; areaId=19; unpl=JF8EALJnNSttCktQUUlSExAQT19VW1oMTh8CPDRWUw8NTVRWSAVPFUJ7XlVdWBRKEB9sZhRUXFNPVQ4YBysSEXteU11bD00VB2xXXAQDGhUQR09SWEBJJV1UW1kATxIHbW4AZG1bS2QFGjIbFBBCVFBeXw9JFAZsbwNVXFFOVwcaMhoiF3ttZFhZDkITBF9mNVVtGh8IDR0FHBAVBl1SXlQBTxcBaGUGUV5QTVUEEgcYEBF7XGRd; __jdv=76161171|haosou-search|t_262767352_haosousearch|cpc|11459545384_0_c15eca931530465490bbb6cd71bb6e6c|1718851824725; user-key=2abe7148-6931-4ee6-b2e0-0693affceb9c; ipLoc-djd=19-1611-19920-19972; PCSYCityID=CN_440000_440500_0; mba_muid=1718851363242970084230; flash=2_92rXKDtL_rwjP0kV8cUqJj5z9_VkSwnfr5B9qO3xg6MYlkPGnJHnVCygbKDJZrYzmalAQpmrqrnIOtNtHJOnn8SXgoXh4C9h9KsCYp-yz-8Kt9NthH7l1dd05ylu4u9c9B9-5SbxbPEN2mWJM0Y1-pSyL1YN0gqgoXMT7GPUXt5*; __jd_ref_cls=LoginDisposition_Go; x-rp-evtoken=N-nAb5Oj6OS1u8hkvixIgFLrinM2vqKAvWWsWYF2PDkYwAFMbpZ_FHgsdV4sCnQjKcMwCfPKAzdW1FbfYr-AAwkKylPFCwPxv5sW1DdgYZi6HlYLF4eDwpiXt0qab5gwDrWqqASULvdgfP_FfOx8LQnXqhH3eJn_kcYk3lU-HN5wKPrr_VWu4A_jEUUTX8p7ZhgINBFvifeJk1yKeJCfMaSUU6GTjxw79H04Su60-wE%3D; jsavif=1; rkv=1.0; xapieid=jdd03QR5ZHLZ7W3P4RYMUU37F5KT74EJG72GAFTLN3KJJDRHRAU46NUODBSYTO7J2T6V4HWGTFLHTBLRDAELSMEOS2JSECUAAAAMQHZXYKIIAAAAAD3BPINROSYRO3MX; avif=1; jsavif=1; __jda=122270672.1718851363242970084230.1718851363.1719032799.1719043094.12; __jdb=122270672.2.1718851363242970084230|12.1719043094; __jdc=122270672; 3AB9D23F7A4B3CSS=jdd03QR5ZHLZ7W3P4RYMUU37F5KT74EJG72GAFTLN3KJJDRHRAU46NUODBSYTO7J2T6V4HWGTFLHTBLRDAELSMEOS2JSECUAAAAMQH3Z7V4YAAAAACPOJFIOCLZ3HCEX; _gia_d=1; shshshfpb=BApXcVWv7PfVAd6uBwC1JM2vPV0jDfB96BxVyJSgW9xJ1Mv6sboC2; 3AB9D23F7A4B3C9B=QR5ZHLZ7W3P4RYMUU37F5KT74EJG72GAFTLN3KJJDRHRAU46NUODBSYTO7J2T6V4HWGTFLHTBLRDAELSMEOS2JSECU'
        # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        # 'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.5,zh;q=0.3',
        # 'Referer': 'https://www.jd.com/',
        # 'DNT': '1',
        # 'Connection': 'keep-alive',
        # 'Upgrade-Insecure-Requests': '1',
        # 'TE': 'Trailers',
    }
df = pd.DataFrame()
for brand in get_brands():
    df_ = main(brake_type, material, brand, headers=headers)
    df = merge_df([df, df_])
df.to_excel('jd_comments_%s_%s.xlsx' % (name_dict[brake_type], name_dict[material]), index=False)
import time
import pandas as pd
from requests_html import HTMLSession

# read products
df = pd.read_excel("jd_products_filtered.xlsx")
df['material'] = None
df['brake type'] = None

headers = {
    'Cookie': 'pinId=iuXMAaF7pVkB-ZaUy57Ew7V9-x-f3wj7; shshshfpa=3f810591-326a-a787-d906-3e3f78baaea0-1657332207; shshshfpx=3f810591-326a-a787-d906-3e3f78baaea0-1657332207; pin=jd_4a800764b8169; unick=%E5%B8%85%E6%B0%94%E7%9A%84%E5%B0%8F%E4%B9%8C%E9%BE%9F11; _tp=A10MK3vojkhCxe%2F%2FR6NZNahNbdoQZNVWIJpZqHFSRcE%3D; _pst=jd_4a800764b8169; __jdu=1718851363242970084230; TrackID=1vZnuwwtov_A15EyBX1Ptq6KsgHLQjsvXkP2K9VjXkknB1MSE_W3BfYuDEAsHK_M0K1M7XM1C5pKaDS6OWo_PNVCjmtdENTj5B0RmOB1yfPw; thor=3126B90478C57763EFC4D4DE5D592AA0D7091767C9D65192EA5F7B1336BDC11B3C5BACE47E73A11404E817A68AD2BBBAAFD8D65E955AAD28190D58F3C8546FD079E3AF0554BECED353BC3FBB12A0277726CA229A15DD031BB5D87D0112E2BF6210A55ECD8F87A32794656E5EE54CEDB1C8D719A4964AAAD44F1D4E75346C2BE1F68AB2005BEA105EC5AC826768933F9872A8DC77CA5429E4CBCA204F764AFA56; areaId=19; unpl=JF8EALJnNSttCktQUUlSExAQT19VW1oMTh8CPDRWUw8NTVRWSAVPFUJ7XlVdWBRKEB9sZhRUXFNPVQ4YBysSEXteU11bD00VB2xXXAQDGhUQR09SWEBJJV1UW1kATxIHbW4AZG1bS2QFGjIbFBBCVFBeXw9JFAZsbwNVXFFOVwcaMhoiF3ttZFhZDkITBF9mNVVtGh8IDR0FHBAVBl1SXlQBTxcBaGUGUV5QTVUEEgcYEBF7XGRd; __jdv=76161171|haosou-search|t_262767352_haosousearch|cpc|11459545384_0_c15eca931530465490bbb6cd71bb6e6c|1718851824725; user-key=2abe7148-6931-4ee6-b2e0-0693affceb9c; ipLoc-djd=19-1611-19920-19972; 3AB9D23F7A4B3C9B=QR5ZHLZ7W3P4RYMUU37F5KT74EJG72GAFTLN3KJJDRHRAU46NUODBSYTO7J2T6V4HWGTFLHTBLRDAELSMEOS2JSECU; PCSYCityID=CN_440000_440500_0; mba_muid=1718851363242970084230; __jda=181111935.1718851363242970084230.1718851363.1718936467.1718946431.8; __jdc=181111935; mba_sid=17189464310155715797676777916.1; __jd_ref_cls=LoginDisposition_Go; x-rp-evtoken=N-nAb5Oj6OS1u8hkvixIgFLrinM2vqKAvWWsWYF2PDkYwAFMbpZ_FHgsdV4sCnQjKcMwCfPKAzdW1FbfYr-AAwkKylPFCwPxv5sW1DdgYZiNOK8TTOkIYyg1EfOC6nbhDmdRsYicfCGZKiAEDJUYy1UO1keFpGi5au3tS6GBAkoHhfuVj0SuGU3MTYw3AGiPcLDY03L0tSSwxCXIcaDLjQmAkoKAJfJ4KcKk9lsHdWk%3D; token=42b2d6ffc9752d0602b69f39e5e86e98,3,954970; __tk=b837750afd435f9823381479c59728a7,3,954970; jsavif=1; jsavif=1; 3AB9D23F7A4B3CSS=jdd03QR5ZHLZ7W3P4RYMUU37F5KT74EJG72GAFTLN3KJJDRHRAU46NUODBSYTO7J2T6V4HWGTFLHTBLRDAELSMEOS2JSECUAAAAMQHFAID5IAAAAADD3YO5TVXXLN4IX; _gia_d=1; __jdb=181111935.5.1718851363242970084230|8.1718946431; flash=2_XUmutiCQUlEgLhIA7QWVZydN-bnfhQV2u_x6LSMC2tzr2N3M_tpDKtHXWisRm9tN22BrLrdbsa0NaIhcoYRx2NDmBEZhfeQ5QNKnul7tj00xDbe1e4B37RaFOYdY0VCiGNSJxK1DPd8QphH3syn1_XxJiTD3lKjWhfbk5WPz_yD*; shshshfpb=BApXckB5IOvVAd6uBwC1JM2vPV0jDfB96BxVyJSgV9xJ1Mv6sboC2',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}

start = input("Start Number:")
data = []

# iterate goods
for id in list(df.index)[int(start):]:

    if id % 20 == 0:
        time.sleep(5)

    url = df.loc[id, 'link']
    print("=======================================")
    print("ID: %d, URL: %s" % (id, url))
    
    # get config
    try:
        # get config
        session = HTMLSession()
        time.sleep(0.1)
        r = session.get(url, headers=headers)
        material = r.text.split('材质：')[1].split('<')[0]
        brake_type = r.text.split('类别：')[1].split('<')[0]
        
        l = list(df.loc[id].values)
        l.append(material)
        l.append(brake_type)
        data.append(l)

        # brand = r.text.split('品牌：')[1].split("target=\'_blank\'>")[1].split('</a>')[0]
        # df.loc[id, 'brand'] = brand
        # df.loc[id, 'brake type'] = brake_type
        # df.loc[id, 'material'] = material
        # df.to_excel('jd_config.xlsx', index=False)
    except:
        print("No Config!")
        break

# save
print("=======================================")
# print("saving...")
col = list(df.columns)
col.append('material')
col.append('brake type')
df_ = pd.DataFrame(data, columns=col)
df_.to_excel('./jd_config_%d_%d.xlsx' % (int(start), id-1), index=False)
print("End at %d" % id-1)
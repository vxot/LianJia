import json


def get_all_page(soup):
    page_url_list = []
    page_div = soup.find('div', attrs={'class': 'house-lst-page-box'})
    if page_div and hasattr(page_div, 'attrs'):
        page_attrs = page_div.attrs
        if 'page-data' in page_attrs:
            page_json = json.loads(page_attrs['page-data'])
            total_page = page_json['totalPage']
            cur_page = page_json['curPage']
        if 'page-url' in page_attrs:
            page_url = page_attrs['page-url']
        if total_page and cur_page and page_url:
            i = 2
            while i <= total_page:
                page_url_list.append(page_url.format(page=i))
                i += 1
        else:
            print('error get total page')
    return page_url_list


def get_url_title(div):
    a_arr = div.select('div.title a')
    if len(a_arr) > 0:
        a = a_arr[0]
        title = a.get_text()
        url = a['href']
        url = url[url.rfind('/') + 1: url.find('.html')]
        return url, title
    return None

import json

from createTable import House, XiaoQu


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


def reset_xiao_qu_status(sql_session, is_breaking=False):
    # 查询之前把house 的 status 全部致0，如果页面中有该house ,把status修改为 1
    # 以此标记那些房源还存在，那些可能下架，或者成交
    houses = sql_session.query(House)
    houses.update({House.status: False})
    sql_session.commit()

    # 断点，清空已经爬过的小区
    if not is_breaking:
        xiao_qus = sql_session.query(XiaoQu)
        for item in xiao_qus:
            item.status = False
    sql_session.commit()

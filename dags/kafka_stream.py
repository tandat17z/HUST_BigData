from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from datetime import datetime, timedelta

# -------------------CRAWL DATA-----------------------------------------------------------
import requests
from bs4 import BeautifulSoup

TIME_SLEEP = 30
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive"
}

def get_list_link(start, end):
    links = []
    for i in range(start, end + 1):
        links.append(
            "https://www.topcv.vn/tim-viec-lam-it-phan-mem-c10026?salary=0&exp=0&company_field=0&sort=up_top&page=" + str(
                i))
    return links

def get_titles(list_link):
    titles = []
    for link in list_link:
        print("Link page: " + link)
        while True:
            response = requests.get(link, headers = headers)
            if response.status_code == 429:
                print(f'sleep {TIME_SLEEP} s')
                time.sleep(TIME_SLEEP)
                continue
            
            print(response.status_code)
            print(response)
            soup = BeautifulSoup(response.content, "html.parser")
            title = soup.findAll('h3', class_='title')
            for tit in title:
                titles.append(tit)
            break
    return titles

def get_links_company(titles):
    links_company = []
    for link_company in titles:
        link_obj = link_company.find('a', href=True)
        if link_obj != None:
            link = link_obj['href']
            links_company.append(link)
    return links_company

def add_contents(contents, data):
    node_tag = contents.find('div', class_="job-tags")
    if node_tag:
        text = node_tag.find('a').get_text(strip = True)
        data['tags'] = text

    node_job_description = contents.find('div', class_='job-description')
    for header in node_job_description.find_all('h3'):
        item_name = header.get_text(strip=True)

        # duyệt qua các node cùng cấp tiếp theo lấy tất cả các text
        nextNode = header
        content = ""
        while True:
            nextNode = nextNode.nextSibling
            if nextNode is None:
                break
            
            content += " ".join(nextNode.stripped_strings)
        data[item_name] = content

def get_data():
    data = {
        "id": time.time(),
        "name": "Công ty Cổ phần S.I.S Việt Nam",
        "mo_ta_cong_viec": "Lập trình viên :· Trực tiếp tham gia vào các dự án phát triển sản phẩm CRM, ERP… của Công ty trên nền tảng C#, MVC 5, ASP.NET, Winform· Tham gia làm rõ nghiệp vụ, thiết kế giải pháp, phát triển nâng cấp hệ thống theo yêu cầu· Tham gia review thiết kế, review code, tối ưu hệ thống đáp ứng lưu lượng truy cập cao· Nghiên cứu áp dụng công nghệ mới nâng cao chất lượng, tối ưu nguồn lực phát triểnKĩ thuật bảo hành phần mềm· Hỗ trợ, xử lý kĩ thuật cho khách hàng trong quá trình sử dụng phần mềm do SISVN cung cấp· Tư vấn các giải pháp để khách hàng sử dụng phần mềm một cách tối ưu và hiệu quả nhất",
        "yeu_cau_cong_viec": "· Tốt nghiệp chuyên ngành CNTT· Ưu tiên ứng viên có hiểu biết về kế toán và quản trị doanh nghiệp",
        "quyen_loi": "· Lương cứng (12 - 18 triệu) + % thưởng theo dự án· Thưởng quý cao theo hiệu quả công việc· Được làm việc trong môi trường nhiều thử thách, chuyên nghiệp nhưng hòa đồng, có cơ hội được đào tạo nâng cao nghiệp vụ chuyên môn thường xuyên, đặc biệt là chuyên ngành công nghệ và kế toán· Được tham gia các dự án lớn, quy mô, ở nhiều tỉnh/ thành phố tại Việt Nam· Chế độ BHXH theo QĐ của nhà nước · Nghỉ phép năm 12 ngày/năm và lễ tết theo quy định của Công ty.· Các chế độ du lịch, team building hàng năm, văn hóa thể thao theo quy định công ty",
        "cach_thuc_ung_tuyen": "Ứng viên nộp hồ sơ trực tuyến bằng cách bấmỨng tuyển ngaydưới đây.ỨNG TUYỂN NGAYLƯU TINHạn nộp hồ sơ: 08/08/2022"
    }
    return data
    # while True:
    #     response_news = requests.get(link, headers = headers)
    #     if response_news.status_code == 429:
    #         print(f'sleep {TIME_SLEEP} s')
    #         time.sleep(TIME_SLEEP)
    #         continue
    #     break
        
    # soup = BeautifulSoup(response_news.content, "html.parser")

    # # lấy tên công ty
    # name_label = soup.find('h2', class_="company-name-label")
    # if name_label is None:
    #     return None
    
    # name = name_label.find('a').get_text(strip=True)
    # # lấy các thông tin về job
    # contents = soup.find("div", class_="job-detail__information-detail")

    # company_data = {}
    # company_data['name'] = name
    # add_contents(contents, company_data)

    # return company_data


# ------------------------------------------------------------------------------

default_args = {
    'owner': 'tandat17z',
    'start_date': datetime(2024, 12, 16, 10, 00),
    'retries': 5,
    'retry_delay': timedelta(minutes = 2)
}

def stream_data():
    import json
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    # links = get_list_link(1, 2)
    # title = get_titles(links)
    # print("title: " + str(title))
    # links_company = get_links_company(title)
    # for link in links_company:
    #     print("Link job: " + link)
    #     try:
    #         res = get_data(link)
    #         print(res)
    #         if res is not None:
    #             producer.send('job_information', json.dumps(res).encode('utf-8'))
    #     except Exception as e:
    #         print(f'An error occured: {e}')
    #         continue

    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60: #1 minute
            break

        try:
            res = get_data()

            producer.send('job_information_1', json.dumps(res).encode('utf-8'))
            time.sleep(5)
        except Exception as e:
            print(f'An error occured: {e}')

    # res = get_data()
    # res = format_data(res)
    # producer.send('users_created', json.dumps(res).encode('utf-8'))

with DAG('kafka_stream_topic_1',
         default_args=default_args,
         description = "This is kafka stream task.",
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_job_data',
        python_callable=stream_data
    )

    streaming_task
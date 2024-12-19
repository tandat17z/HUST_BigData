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
        'id': time.time(),
        "name": "Công ty TNHH Tap Hospitality Vietnam",
        "tags": "Chuyên môn: Software Engineer",
        "description": "•\tTham gia phát triển phần mềm cho các Tập đoàn khách sạn, resort hàng đầu tại Nhật Bản. •\tTeamwork, họp bàn, phát triển sản phẩm cùng với đội ngũ phía Nhật Bản. •\tMaintain code hiện tại và phát triển các chức năng mới. •\tBáo cáo công việc với cấp trên.",
        "yeu_cau_cong_viec": "1- Với lập trình viên Java: •\tNắm vững kiến thức lập trình Java căn bản: Java core, OOP... •\tCó kinh nghiệm lập trình Angular. •\tThành thạo các hệ quản trị cơ sở dữ liệu: PostgreSQL, IBM DB2... •\tTiếng Anh đọc hiểu cơ bản. 2- Với lập trình viên VueJs/Angular: •\tTừ một năm kinh nghiệm lập trình VueJs/Angular •\tThuần thục CSS/SCSS, HTML, Javascript, TypeScript •\tCó kinh nghiệm về UX, UI, State Management, Functional Programming là một lợi thế. •\tCó kinh nghiệm làm việc với Web Application, API, DataBase. •\tHiểu rõ authentication, RESTful API, Git, Virtual Machine và quy trình phát triển phần mềm •\tCó kiến thức cơ bản, kinh nghiệm về các dịch vụ AWS, Microservices, Docker, Kubernetes và cơ sở hạ tầng đám mây (Cloud Infrastructure) 3- Với lập trình viên C# •\tCó ít nhất 1 năm kinh nghiệm lập trình với C#.Net (Winform,...) •\tThành thạo PostgreSQL, ưu tiên ứng viên có hiểu biết và kinh nghiệm với IBM DB2 và SQL Server •\tCó hiểu biết về VB6, VB.Net là lợi thế •\tBiết tiếng Nhật là 1 lợi thế",
        "quyen_loi": "Thời gian làm việc: Từ thứ 2 đến thứ 6, nghỉ Thứ 7 & Chủ Nhật. Đặc biệt bạn có thể tự chủ động giờ bắt đầu làm việc (đến trước 9h sáng), đến sớm về sớm. Miễn đảm bảo đủ 8 tiếng/ngày và tiến độ dự án. Kết hợp linh hoạt giữa làm tại văn phòng và remote Mức lương cạnh tranh (tùy theo kỹ năng và kinh nghiệm) Review tăng lương  hàng năm Thưởng Tết, thưởng dự án Thưởng các ngày lễ: Tết Nguyên Đán, quà sinh nhật, Trung thu, 30/4, 2/9, 8/3, 20/10... Trợ cấp ăn trưa, cung cấp cung cấp trà, cà phê, trợ cấp đi lại, trợ cấp ngoại ngữ v.v.. Thanh toán OT theo quy định của luật lao động Có cơ hội đi onsite tại Tokyo hoặc Okinawa ngắn và dài hạn Bảo hiểm full lương Ngày phép: 12 ngày/năm và tăng dần theo thời gian làm việc Du lịch công ty hàng năm Khám sức khỏe định kỳ 01 lần/năm tại BV Quốc tế Hoạt động giải trí và Team Building phong phú (Câu lạc bộ bóng đá, chạy bộ, đạp xe, PES...)",
        "dia_diem_lam_viec": "- Hà Nội: Tầng 10 Toà nhà CMC, 11 Phố Duy Tân, Dịch Vọng Hậu, Cầu Giấy",
        "cach_thuc_ung_tuyen": "Ứng viên nộp hồ sơ trực tuyến bằng cách bấm Ứng tuyển ngay dưới đây."
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

            producer.send('job_information', json.dumps(res).encode('utf-8'))
            time.sleep(15)
        except Exception as e:
            print(f'An error occured: {e}')

    # res = get_data()
    # res = format_data(res)
    # producer.send('users_created', json.dumps(res).encode('utf-8'))

with DAG('kafka_stream_job_data_1',
         default_args=default_args,
         description = "This is kafka stream task.",
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_job_data',
        python_callable=stream_data
    )

    streaming_task
from datetime import datetime, timedelta
from io import BytesIO
import json
import pycurl
import os
import time


class ElasticsearchPing:
    """
    Elasticsearch Node Server Ping 프로그램 - Linux 콘솔용
    현재 동작여부만 판단하여 알려줌
    """
    def __init__(self):
        """
        내부 사용 번수 설정
        """
        # Elasticsearch API URL
        self.elasticsearch_userpw = "elastic:elastic1#"
        self.elasticsearch_server = [
            'https://elastic-01:9200',
            'https://elastic-02:9200',
            'https://elastic-03:9200'
        ]
        self.ansi_formatters = {
            # ANSI Controll 문자
            "CEND": "\033[0m",
            "CBOLD": "\033[1m",
            "CITALIC": "\033[3m",
            "CURL": "\033[4m",
            "CBLINK": "\033[5m",
            "CBLINK2": "\033[6m",
            "CLELECTED": "\033[7m",
            # ANSI 글자색
            "CBLACK": "\033[30m",
            "CRED": "\033[31m",
            "CGREEN": "\033[32m",
            "CYELLOW": "\033[33m",
            "CBLUE": "\033[34m",
            "CVIOLET": "\033[35m",
            "CBEIGE": "\033[36m",
            "CWHITE": "\033[37m",
            "CGREY": "\033[90m",
            "CRED2": "\033[91m",
            "CGREEN2": "\033[92m",
            "CYELLOW2": "\033[93m",
            "CBLUE2": "\033[94m",
            "CVIOLET2": "\033[95m",
            "CBEIGE2": "\033[96m",
            "CWHITE2": "\033[97m",
            # ANSI 배경색
            "CBLACKBG": "\033[40m",
            "CREDBG": "\033[41m",
            "CGREENBG": "\033[42m",
            "CYELLOWBG": "\033[43m",
            "CBLUEBG": "\033[44m",
            "CVIOLETBG": "\033[45m",
            "CBEIGEBG": "\033[46m",
            "CWHITEBG": "\033[47m",
            "CBLACKBG2": "\033[100m",
            "CREDBG2": "\033[101m",
            "CGREENBG2": "\033[102m",
            "CYELLOWBG2": "\033[103m",
            "CBLUEBG2": "\033[104m",
            "CVIOLETBG2": "\033[105m",
            "CBEIGEBG2": "\033[106m",
            "CWHITEBG2": "\033[107m"
        }

    def call_elasticsearch_by_pycurl(self, url: str) -> str:
        """
        pyCurl 을 사용한 Elasticsearch 호출
        :param url: 호출 할 Elasticsearch URL
        :return: 응답결과 전달
        """
        # StringIO 가 안되는 경우 BytesIO 를 사용
        result = BytesIO()
        conn = pycurl.Curl()
        conn.setopt(pycurl.URL, url)
        conn.setopt(pycurl.HTTPGET, True)
        conn.setopt(pycurl.SSL_VERIFYPEER, False)
        conn.setopt(pycurl.USERPWD, self.elasticsearch_userpw)
        conn.setopt(pycurl.WRITEFUNCTION, result.write)
        # 실행
        conn.perform()
        res = result.getvalue().decode('UTF-8')
        # 반드시 Close 할 것!!
        result.close()
        conn.close()
        return res

    def check_ping_result(self, result: str) -> str:
        """
        Elasticsearch Node 서버 응답 확인
        :param result: pyCurl 호출 응답 str을 확인 하여 서버 상태를 반환
        :return: True : 서버 상태 OK, False : 서버 상태 DOWN
        """
        if result.startswith("{"):
            tmp_json = json.dumps(result)
            if "error" in tmp_json:
                return "{CRED}DOWN{CEND}".format(**self.ansi_formatters)
            elif "name" in tmp_json:
                return "{CGREEN}OK{CEND}".format(**self.ansi_formatters)
            else:
                return "{CRED}DOWN{CEND}".format(**self.ansi_formatters)
        else:
            return "{CRED}DOWN{CEND}".format(**self.ansi_formatters)

    @staticmethod
    def change_kor_time(now):
        """
        주어진 시간을 한국시간으로 변경
        :param now: utc time
        :return: 한국시간
        """
        time_gap = timedelta(hours=9)
        return now + time_gap

    def elasticsearch_ping(self):
        """
        변수에 설정된 Elasticsearch Node 서버 목록 전체를 호출하여 현재 상태 확인
        :return: 없음
        """
        count = 0
        title = ""
        message = ""
        for url in self.elasticsearch_server:
            start_time = datetime.utcnow()
            try:
                result = self.call_elasticsearch_by_pycurl(url)
            except Exception as err:
                result = str(err)
            end_time = datetime.utcnow()
            diff_time = end_time - start_time
            server_status = self.check_ping_result(result)
            if count == 0:
                title = "ping time : %s" % self.change_kor_time(start_time)
            message = message + "server : %s, req time : %s seconds, status : %s\n" % (url, diff_time, server_status)
        os.system("clear")
        print(title)
        print(message)


if __name__ == '__main__':
    """
    실행 명령어 : /home/apidev/anaconda3/envs/enroll_list_restapi/bin/python /home/apidev/anaconda3/envs/enroll_list_restapi/script/elasticsearch_ping_monitoring.py
    """
    ping = ElasticsearchPing()
    while True:
        ping.elasticsearch_ping()
        time.sleep(5)

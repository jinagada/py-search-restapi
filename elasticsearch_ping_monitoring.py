from datetime import datetime, timedelta
from io import BytesIO
import json
import pycurl
import os
import time
import asyncio
import subprocess


class ElasticsearchPing:
    """
    Elasticsearch Node Server Ping 프로그램 - Linux 콘솔용
    현재 동작여부만 판단하여 알려줌
    서버 확인을 비동기 방식으로 변경
    """
    def __init__(self):
        """
        내부 사용 번수 설정
        """
        # Async 객체 선언
        self.loop = asyncio.get_event_loop()
        # Elasticsearch API URL
        self.elasticsearch_userpw = "elastic:elastic1#"
        self.elasticsearch_server = [
            'https://elastic-01:9200',
            'https://elastic-02:9200',
            'https://elastic-03:9200'
        ]
        self.server_id = "hunetelk"
        self.server_pw = "hunetelk!"
        self.ansi_formatters = {
            # ANSI Controll 문자
            "CEND": "\033[0m",
            "CBOLD": "\033[1m",
            "CITALIC": "\033[3m",
            "CURL": "\033[4m",
            "CBLINK": "\033[5m",
            "CBLINK2": "\033[6m",
            "CSELECTED": "\033[7m",
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
        try:
            conn.setopt(pycurl.URL, url)
            conn.setopt(pycurl.HTTPGET, True)
            conn.setopt(pycurl.SSL_VERIFYPEER, False)
            conn.setopt(pycurl.USERPWD, self.elasticsearch_userpw)
            conn.setopt(pycurl.WRITEFUNCTION, result.write)
            # 실행
            conn.perform()
            res = result.getvalue().decode('UTF-8')
        except pycurl.error as err:
            raise err
        except Exception as err:
            raise err
        finally:
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

    def elasticsearch_ping(self, server_url):
        """
        변수에 설정된 Elasticsearch Node 서버를 호출하여 현재 상태 확인
        PyCulr 호출 후 추가 정보를 같이 반환하기 위해서 별도의 메서드로 작성
        :param server_url: Elasticsearch Node Server Url
        :return: url, diff_time, elastic_status, server_info
        """
        url = server_url
        start_time = datetime.utcnow()
        try:
            result = self.call_elasticsearch_by_pycurl(url)
        except Exception as err:
            result = str(err)
        end_time = datetime.utcnow()
        diff_time = end_time - start_time
        elastic_status = self.check_ping_result(result)
        server_info = self.elasticsearch_server_ping(server_url)
        return url, diff_time, elastic_status, server_info

    def elasticsearch_server_ping(self, server_url):
        """
        변수에 설정된 Elasticsearch Node 서버의 하드웨어 상태 확인
        :param server_url: Elasticsearch Node Server Url
        :return: cpu 사용율, 메모리 사용율, 디스크 사용율
        """
        login = "sshpass -p '%s' ssh -o StrictHostKeyChecking=no %s@%s"\
                   % (self.server_pw, self.server_id, server_url[8:-5])
        command_cpu = login + " top -b -n 10 -d.2 | grep 'Cpu' | awk 'NR==3{printf \"%s\", $2 }'"
        cpu = subprocess.check_output(command_cpu, shell=True)
        command_mem = login + " free -m | awk 'NR==2{printf \"%.2f%%\", $3*100/$2 }'"
        mem = subprocess.check_output(command_mem, shell=True)
        command_disk = login + " df -h | awk '$NF==\"/\"{printf \"%s\", $5 }'"
        disk = subprocess.check_output(command_disk, shell=True)
        result = "cpu : {}%, mem : {}, disk : {}".format(str(cpu)[2:-1], str(mem)[2:-1], str(disk)[2:-1])
        return result

    async def print_ping(self, server_url):
        """
        변수에 설정된 Elasticsearch Node 서버를 호출하여 현재 상태 확인 후 결과 값 print
        PyCurl은 Async 방식을 지원하지 않기 때문에 asyncio 를 사용하여 강제로 Async 방식으로 실행
        :param server_url: Elasticsearch Node Server Url
        :return: url, diff_time, server_status
        """
        # run_in_executor 를 사용하여 강제로 Async 방식을 사용
        url, diff_time, elastic_status, server_info = await self.loop.run_in_executor(None, self.elasticsearch_ping,
                                                                                      server_url)
        print("server : %s, elastic res time : %s seconds, status : %s, %s"
              % (url, diff_time, elastic_status, server_info))

    def ping_all(self):
        """
        서버 목록에 있는 모든 Elasticsearch Node 서버의 상태 확인
        Async 방식으로 작성한 print_ping 을 호출 하여 각 서버의 응답을 먼저 확인 되는 순서로 보여줌
        :return: 없음
        """
        os.system("clear")
        print("ping time : %s" % self.change_kor_time(datetime.utcnow()))
        # asyncio 를 사용하여 서버 목록을 Async 방식으로 전달
        futures = [self.print_ping(url) for url in self.elasticsearch_server]
        self.loop.run_until_complete(asyncio.wait(futures))


if __name__ == '__main__':
    """
    실행 명령어 : /home/apidev/anaconda3/envs/project/bin/python /home/apidev/anaconda3/envs/project/script/elasticsearch_ping_monitoring.py
    """
    ping = ElasticsearchPing()
    while True:
        ping.ping_all()
        time.sleep(5)

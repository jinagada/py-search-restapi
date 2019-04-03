from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
import socket
import json
import traceback
import time


class KafkaTopicCrawling:
    """
    Kafka Manager의 Topic List 크롤링 하기
    """
    def __init__(self):
        """
        공통 변수 설정
        """
        # Kafka Manager Topic List Url
        self.topic_list_url = "http://yourip1:9010/clusters/yourkafkacluster/topics"
        self.logstash_server_info = ("yourip2", 5046)

    def crawling(self):
        """
        Topic List를 크롤링하여 producer_message_per_sec 기준으로 내림차순 정렬
        데이터를 TCP 전송을 위한 byte 문자열로 반환
        :return:
        """
        try:
            # url 접속 후 html 데이터 추출
            html = urlopen(self.topic_list_url)
            # html 파싱
            bs_object = BeautifulSoup(html, "html.parser")
            # Topic List 목록만 추출
            topic_list = bs_object.find("table", {"id": "topics-table"}).find("tbody").find_all("tr")
            # 결과 목록
            topic_infos = []
            # 추출 시간 설정
            crawling_time = datetime.now()
            now = crawling_time.strftime("%Y-%m-%d %H:%M:%S")
            # 각 데이터 추출
            for row in topic_list:
                info = row.find_all("td")
                topic_infos.append(
                    {
                        "topic": info[0].text,
                        "partitions": int(info[1].text),
                        "brokers": int(info[2].text),
                        "brokers_spread_ratio": int(info[3].text),
                        "brokers_skew_ratio": int(info[4].text),
                        "brokers_leader_skew_ratio": int(info[5].text),
                        "replicas": int(info[6].text),
                        "under_replicated_ratio": int(info[7].text),
                        "producer_message_per_sec": float(info[8].text),
                        "summed_recent_offsets": int(info[9].text.replace(",", "")),
                        "crawling_time": now,
                    }
                )
            # 추출된 데이터를 producer_message_per_sec 기준으로 내림차순 정렬
            topic_infos_sort = sorted(topic_infos, key=lambda per_sec: per_sec["producer_message_per_sec"], reverse=True)
            # json string 으로 변환
            result = json.dumps(topic_infos_sort)
            # TCP 전송을 위한 byte 문자로 변환하여 반환
            result_byte = bytes(result, encoding="UTF-8")
        except Exception as err:
            traceback.print_exc()
            raise err
        return result_byte

    def tcp_send(self, byte_str):
        """
        크롤링한 데이터를 Logstash TCP 포트로 전송
        :param byte_str: 크롤링한 데이터
        :return:
        """
        logstash_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            logstash_tcp.connect(self.logstash_server_info)
            logstash_tcp.sendall(byte_str)
        except Exception as err:
            traceback.print_exc()
            raise err
        finally:
            logstash_tcp.close()

    def crawl_and_send(self):
        """
        Kafka Topic List 크롤링 처리
        :return:
        """
        try:
            data = self.crawling()
            self.tcp_send(data)
        except Exception as err:
            print("%s Kafka Topic List Crawling ERROR : %s" % (str(datetime.now()), err))


if __name__ == '__main__':
    """
    실행 명령어 :
    /home/yourid/anaconda3/envs/project/bin/python /home/yourid/anaconda3/envs/project/script/kafka_topic_crawling.py
    """
    crawling = KafkaTopicCrawling()
    while True:
        crawling.crawl_and_send()
        time.sleep(5)

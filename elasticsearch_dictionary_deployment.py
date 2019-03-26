from datetime import datetime
from io import BytesIO
from random import randint
import json
import pycurl
import subprocess
import traceback
import time


class DictionaryDeployment:
    """
    Elasticsearch Nori 형태소 분석기의 사용자 정의 사전, 동의어 사전 등을 배포 후 Index 에 적용
    """
    def __init__(self):
        """
        내부사용 변수 설정
        """
        # Elasticsearch API URL
        self.elasticsearch_userpw = "elastic:elastic1#"
        self.elasticsearch_server = [
            'https://elastic-01:9200',
            'https://elastic-02:9200',
            'https://elastic-03:9200'
        ]
        # Elasticsearch Node 서버 계정
        self.server_id = "yourid"
        self.server_pw = "yourid!"
        # 배포용 사전파일 위치
        self.nori_dictionary_path = "/home/yourid/anaconda3/envs/project/script/nori_dictionary"
        # Elasticsearch Node 서버 사전파일 위치
        self.node_server_dictionary_path = "/home/yourid/local/elasticsearch/config"
        # 적용할 Elasticsearch Index 목록
        self.index_alias = [
            "test-index-1",
            "test-index-2"
        ]

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
            conn.setopt(pycurl.POST, True)
            conn.setopt(pycurl.SSL_VERIFYPEER, False)
            conn.setopt(pycurl.USERPWD, self.elasticsearch_userpw)
            conn.setopt(pycurl.WRITEFUNCTION, result.write)
            # 파라메터 미설정
            conn.setopt(pycurl.POSTFIELDS, "")
            conn.unsetopt(pycurl.HTTPHEADER)
            # 실행
            conn.perform()
            res = result.getvalue().decode('UTF-8')
        except pycurl.error as err:
            traceback.print_exc()
            raise err
        except Exception as err:
            traceback.print_exc()
            raise err
        finally:
            # 반드시 Close 할 것!!
            result.close()
            conn.close()
        return res

    def call_copy_command(self, server_url, local_path, node_path):
        """
        scp를 사용하여 서버에 파일 복사
        :param server_url: 복사 대상 서버
        :param local_path: 복사 할 파일 경로
        :param node_path: 복사 대상 파일 경로
        :return:
        """
        try:
            print("%s copy start : %s -> %s" % (str(datetime.now()), local_path, node_path))
            copy_command = "sshpass -p '%s' scp -o StrictHostKeyChecking=no %s %s@%s:%s" \
                           % (self.server_pw, local_path, self.server_id, server_url[8:-5], node_path)
            subprocess.check_call(copy_command, shell=True)
            print("%s copy end : %s -> %s" % (str(datetime.now()), local_path, node_path))
        except Exception as err:
            traceback.print_exc()
            raise err

    def dictionary_copy_to_elasticsearch_node(self, server_url):
        """
        사용자정의 사전, 동의어 사전 파일을 각 노드 서버에 복사
        :param server_url: Elasticsearch Node Server Url
        :return:
        """
        try:
            # 사용자 정의 사전 복사
            userdic_ko_local_path = self.nori_dictionary_path + "/userdic_ko.txt"
            userdic_ko_node_path = self.node_server_dictionary_path + "/userdic_ko.txt"
            self.call_copy_command(server_url, userdic_ko_local_path, userdic_ko_node_path)
            # 동의어 사전 복사
            synonym_local_path = self.nori_dictionary_path + "/synonym.txt"
            synonym_node_path = self.node_server_dictionary_path + "/synonym.txt"
            self.call_copy_command(server_url, synonym_local_path, synonym_node_path)
            # 불용어 사전 복사
            stopwords_local_path = self.nori_dictionary_path + "/stopwords.txt"
            stopwords_node_path = self.node_server_dictionary_path + "/stopwords.txt"
            self.call_copy_command(server_url, stopwords_local_path, stopwords_node_path)
        except Exception as err:
            raise err

    def call_endpoint_url(self, endpoint_url):
        """
        endpoint url 호출 후 결과 확인
        :param endpoint_url: 요청할 endpoint url
        :return:
        """
        try:
            print("%s %s start" % (str(datetime.now()), endpoint_url))
            res = self.call_elasticsearch_by_pycurl(endpoint_url)
            result_json = json.loads(res)
            if "error" in result_json and result_json["error"]:
                # 오류 발생 시 Exception 생성
                print(res)
                msg = result_json["error"]["type"] + ":" + result_json["error"]["reason"]
                raise Exception(msg)
            else:
                # 정상 처리
                print("%s %s end" % (str(datetime.now()), endpoint_url))
        except Exception as err:
            raise err

    def apply_dictionary(self, index_name):
        """
        사전 파일 배포 후 각 Index 재설정
        :param index_name: 적용할 Index Name
        :return:
        """
        try:
            server_url = self.elasticsearch_server[randint(0, 2)]
            # Index close
            endpoint_url = "%s/%s/_close" % (server_url, index_name)
            self.call_endpoint_url(endpoint_url)
            # Index open
            endpoint_url = "%s/%s/_open" % (server_url, index_name)
            self.call_endpoint_url(endpoint_url)
            # Index update
            time.sleep(1)  # open 후 1초 정도 기다려야 정상 동작함
            endpoint_url = "%s/%s/_update_by_query" % (server_url, index_name)
            self.call_endpoint_url(endpoint_url)
        except Exception as err:
            raise err

    def deploy_dictionary(self):
        """
        변경된 사전들을 Elasticsearch Node 서버들에 적용
        :return:
        """
        try:
            # 서버에 파일 복제
            for server_url in self.elasticsearch_server:
                self.dictionary_copy_to_elasticsearch_node(server_url)
            # 현재 사용중인 Index에 적용
            for index_name in self.index_alias:
                self.apply_dictionary(index_name)
        except Exception as err:
            print("%s DEPLOY ERROR : %s" % (str(datetime.now()), err))


if __name__ == '__main__':
    """
    실행 명령어 :
    /home/yourid/anaconda3/envs/project/bin/python /home/yourid/anaconda3/envs/project/script/elasticsearch_dictionary_deployment.py
    """
    deploy = DictionaryDeployment()
    deploy.deploy_dictionary()

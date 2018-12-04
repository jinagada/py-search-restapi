from flask import Flask, g
from flask_restful import Resource, Api, reqparse
from werkzeug.local import LocalProxy
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
from kafka.errors import KafkaError
from random import randint
from requests import post
from io import BytesIO
import logging
import json
import datetime
import pycurl

app = Flask(__name__)
api = Api(app)

# create elasticsearch trace log
es_logger = logging.getLogger('elasticsearch.trace')
es_logger.propagate = False
es_logger.setLevel(logging.INFO)
# create console handler
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
# add the handlers to logger
es_logger.addHandler(consoleHandler)

elasticsearch_server = [
    'https://elastic:elastic!@192.168.56.101:9200',
    'https://elastic:elastic!@192.168.56.102:9200',
    'https://elastic:elastic!@192.168.56.103:9200'
]


# Elasticsearch Node Connection
def get_search_conn():
    if '_elasticsearch' not in g:
        g._elasticsearch = Elasticsearch(
            elasticsearch_server,
            ca_certs=False,
            verify_certs=False
        )
    return g._elasticsearch


# Home
@app.route('/')
def hello_world():
    return 'Elasticsearch API Home!!!!'


# 페이징 처리 시 scroll 사용 결과 반환
def make_search_result_scroll(response):
    if '_scroll_id' not in response:
        return {
            "error": "scroll_id not found."
        }
    scroll_id = response['_scroll_id']
    total_count = response['hits']['total']
    res_list = response['hits']['hits']
    num_result = len(res_list)
    data_list = []
    for row in res_list:
        data_list.append(row['_source'])
    return {
        "total_count": total_count,
        "num_result": num_result,
        "scroll_id": scroll_id,
        "datas": data_list
    }


# 페이징 처리 시 from, size 사용 결과 반환
def make_search_result_page(response, page, from_num):
    total_count = response['hits']['total']
    res_list = response['hits']['hits']
    num_result = len(res_list)
    data_list = []
    for row in res_list:
        data_list.append(row['_source'])
    return {
        "total_count": total_count,
        "num_result": num_result,
        "curr_page": page,
        "from_num": from_num,
        "datas": data_list
    }


# 오류 메시지 정의
def make_error_message(error_type):
    if error_type == 1:
        return {
            "error": "Parameter not found."
        }


# 페이지 번호로 from 값 계산
def get_curr_from(page, row_per_page):
    if row_per_page is None:
        row_count = 20
    else:
        row_count = row_per_page
    if page is None:
        curr_page = 1
    else:
        if page < 1:
            curr_page = 1
        else:
            curr_page = page
    if curr_page == 1:
        curr_from = 0
    else:
        curr_from = (row_count * (page - 1))
    return curr_from


# Elasticsearch 기본 Query DSL
def make_basic_query():
    return {
        "from": 0,
        "size": 20,
        "query": {
            "bool": {
                "must": [],
                "must_not": [],
                "should": [],
                "filter": {
                    "bool": {
                        "must": []
                    }
                }
            }
        },
        "sort": [],
        "_source": []
    }


# 파라메터에 따라 Query DSL 생성
def make_search_condition(from_num, keyword_p):
    parser = reqparse.RequestParser()
    parser.add_argument('best_yn')
    parser.add_argument('new_yn')
    parser.add_argument('reg_date')
    parser.add_argument('view_cnt')
    parser.add_argument('s_reg_date')
    args = parser.parse_args()
    body_obj = make_basic_query()
    body_obj['from'] = from_num
    # 검색조건
    if keyword_p is not None:
        body_obj['query']['bool']['must'].append({
            "multi_match": {
                "query": keyword_p,
                "fields": [
                    "goodsname_nm^3",
                    "content",
                    "description"
                ],
                "fuzziness": "AUTO"
            }
        })
    # Filter 조건
    if 'best_yn' in args and args['best_yn']:
        body_obj['query']['bool']['filter']['bool']['must'].append({
            "term": {
                "best_yn.keyword": args['best_yn']
            }
        })
    if 'new_yn' in args and args['new_yn']:
        body_obj['query']['bool']['filter']['bool']['must'].append({
            "term": {
                "new_yn.keyword": args['new_yn']
            }
        })
    if 'reg_date' in args and args['reg_date']:
        body_obj['query']['bool']['filter']['bool']['must'].append({
            "range": {
                'reg_date': {
                    'gte': args['reg_date']
                }
            }
        })
    if 'view_cnt' in args and args['view_cnt']:
        body_obj['query']['bool']['filter']['bool']['must'].append({
            "range": {
                'view_cnt': {
                    'gte': args['view_cnt']
                }
            }
        })
    # sort 조건
    if 's_reg_date' in args and args['s_reg_date']:
        body_obj['sort'].append({
            "reg_date": {
                "order": args['s_reg_date']
            }
        })
    # Output Field 제한
    body_obj['_source'] = [
        "group_nm",
        "goodsname_nm",
        "goods_id",
        "cate1_code",
        "cate2_code",
        "content",
        "description",
        "best_yn",
        "new_yn",
        "user_id",
        "reg_date"
    ]
    return body_obj


# 단어 검색 샘플 API - 페이징 처리 시 scroll 사용
class KeywordSearch(Resource):
    def get(self, keyword_p):
        if keyword_p is None:
            return make_error_message(1)
        body_obj = make_search_condition(0, keyword_p)
        body_obj['query']['bool']['must'].clear()
        body_obj['query']['bool']['must'].append({
            "match": {
                "goodsname_nm": {
                    "query": keyword_p,
                    "fuzziness": "AUTO"
                }
            }
        })
        del body_obj['from']
        del body_obj['size']
        res = search.search(
            index='search-nori-sample1',
            body=body_obj,
            scroll='1m',
            size=20
        )
        return make_search_result_scroll(res)

    # scroll_id 값으로 다음 페이지 조회
    def post(self, keyword_p):
        if keyword_p is None:
            return make_error_message(1)
        parser = reqparse.RequestParser()
        parser.add_argument('scroll_id')
        args = parser.parse_args()
        if 'scroll_id' not in args:
            return make_error_message(1)
        scroll_id = args['scroll_id']
        res = search.scroll(scroll_id=scroll_id, scroll='1m')
        return make_search_result_scroll(res)


# 문장 검색 샘플 API - 페이징 처리 시 scroll 사용
class TextSearch(Resource):
    def get(self, keyword_p):
        if keyword_p is None:
            return make_error_message(1)
        body_obj = make_search_condition(0, keyword_p)
        body_obj['query']['bool']['must'].clear()
        body_obj['query']['bool']['must'].append({
            "match_phrase": {
                "goodsname_nm": {
                    "query": keyword_p,
                    "slop": 5
                }
            }
        })
        del body_obj['from']
        del body_obj['size']
        res = search.search(
            index='search-nori-sample1',
            body=body_obj,
            scroll='1m',
            size=20
        )
        return make_search_result_scroll(res)

    # scroll_id 값으로 다음 페이지 조회
    def post(self, keyword_p):
        if keyword_p is None:
            return make_error_message(1)
        parser = reqparse.RequestParser()
        parser.add_argument('scroll_id')
        args = parser.parse_args()
        if 'scroll_id' not in args:
            return make_error_message(1)
        scroll_id = args['scroll_id']
        res = search.scroll(scroll_id=scroll_id, scroll='1m')
        return make_search_result_scroll(res)


# 단어 검색 샘플 API - 페이징 처리 시 from, size 사용
class KeywordSearchPaging(Resource):
    def get(self, page, keyword_p):
        if page is None:
            page = 1
        from_num = get_curr_from(page)
        if keyword_p is None:
            return make_error_message(1)
        body_obj = make_search_condition(from_num, keyword_p)
        body_obj['query']['bool']['must'].clear()
        body_obj['query']['bool']['must'].append({
            "match": {
                "goodsname_nm": {
                    "query": keyword_p,
                    "fuzziness": "AUTO"
                }
            }
        })
        res = search.search(
            index='search-nori-sample1',
            body=body_obj
        )
        return make_search_result_page(res, page, from_num)

    def post(self, page, keyword_p):
        if page is None:
            page = 1
        from_num = get_curr_from(page)
        if keyword_p is None:
            return make_error_message(1)
        body_obj = make_search_condition(from_num, keyword_p)
        res = search.search(
            index='search-nori-sample1',
            body=body_obj
        )
        return make_search_result_page(res, page, from_num)


# 문장 검색 샘플 API - 페이징 처리 시 from, size 사용
class TextSearchPaging(Resource):
    def get(self, page, keyword_p):
        if page is None:
            page = 1
        from_num = get_curr_from(page)
        if keyword_p is None:
            return make_error_message(1)
        body_obj = make_search_condition(from_num, keyword_p)
        body_obj['query']['bool']['must'].clear()
        body_obj['query']['bool']['must'].append({
            "match_phrase": {
                "goodsname_nm": {
                    "query": keyword_p,
                    "slop": 5
                }
            }
        })
        res = search.search(
            index='search-nori-sample1',
            body=body_obj
        )
        return make_search_result_page(res, page, from_num)


def fn_create_logger():
    global logger
    logger = logging.getLogger('kafka_api')
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s : %(message)s [%(filename)s:%(lineno)s]'
    )
    # 파일 출력 핸들러 만들기
    # 로그파일을 maxBytes(10MB) 까지 기록하고 파일 갯수는 backupCount(10) 수 까지 파일을 남긴다.
    file_handler = logging.handlers.RotatingFileHandler(
        '/var/log/kafka_api/app.log',
        maxBytes=1024 * 1024 * 10,
        backupCount=10
    )
    # 핸들러에 포메터를 지정한다.
    file_handler.setFormatter(formatter)
    # 로거 인스턴스에 파일 핸들러 추가하기
    logger.addHandler(file_handler)
    # 로거 인스턴스 생
    logger.setLevel(logging.DEBUG)


def get_kafka_conn():
    # create logger instance
    if 'logger' not in globals():
        fn_create_logger()
    _producer = getattr(g, 'producer', None)
    if _producer is None:
        _producer = g.producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                                               value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                                                   'utf-8'),
                                               acks=0,
                                               linger_ms=5,
                                               batch_size=500000)
    return _producer


# Kafka API 테스트용
class KafkaIFTest(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic', type=str)
        parser.add_argument('field', type=str)
        args = parser.parse_args()

        print(args['topic'])
        print(args['field'])

        json_txt = args['field'].replace("'", "\"")
        json_txt = json.loads(json_txt)

        # producer.send(topic=args['topic'], value=json_txt)
        # producer.flush()
        print(json_txt)


# 조회를 위한 Index 설정
def get_index(code):
    if code == 1:
        index = "bigginsight"
    else:
        index = "bigginsight"
    return index


# 조회용 SQL 작성
def make_search_sql(code):
    sql = (
        # SELECT Condition
        'SELECT * FROM "%s" WHERE 1 = 1 ' % get_index(code)
        # WHERE Condition
        + " {timestamp} {keyword} {category}"
        # ORDER BY Condition
        + " {sort}"
    ).format(
        timestamp=condition_timestamp(),
        keyword=condition_keyword(),
        category=condition_category(),
        sort=condition_sort()
    )
    return sql


# @timestamp 컬럼 테스트
def condition_timestamp():
    day_100 = datetime.timedelta(days=100)
    day_100_ago = datetime.datetime.now() - day_100
    sql = 'AND "@timestamp" >= \'%s\'' % day_100_ago.strftime('%Y-%m-%dT00:00:00')
    return sql


# 검색어
def condition_keyword():
    parser = reqparse.RequestParser()
    parser.add_argument('keyword')
    args = parser.parse_args()
    if 'keyword' in args and args['keyword']:
        sql = "AND MATCH('username', '%s') " % args['keyword']
    else:
        sql = ""
    return sql


# 카테고리
def condition_category():
    parser = reqparse.RequestParser()
    parser.add_argument('category')
    args = parser.parse_args()
    if 'category' in args and args['category']:
        sql = "AND category = '%s'" % args['category']
    else:
        sql = ""
    return sql


# 정렬조건
def condition_sort():
    return "ORDER BY accessPointId asc"


# Elasticsearch SQL을 Query DSL로 변환(Requests 사용)
def call_elasticsearch_sql_by_requests(sql):
    elasticsearch_sql = {"query": sql}
    url = elasticsearch_server[randint(0, 2)] + '/_xpack/sql/translate'
    res = post(url, json=elasticsearch_sql, verify=False)
    return res.json()


# Elasticsearch SQL을 Query DSL로 변환(pycURL 사용)
def call_elasticsearch_sql_by_pycurl(sql):
    # StringIO 가 안되는 경우 BytesIO 를 사용
    result = BytesIO()
    conn = pycurl.Curl()
    url = elasticsearch_server[randint(0, 2)] + '/_xpack/sql/translate'
    conn.setopt(pycurl.URL, url)
    conn.setopt(pycurl.POST, True)
    conn.setopt(pycurl.SSL_VERIFYPEER, False)
    conn.setopt(pycurl.HTTPHEADER, ['Content-type:application/json;charset=utf-8'])
    # 파라메터 설정
    elasticsearch_sql = {"query": sql}
    conn.setopt(pycurl.POSTFIELDS, json.dumps(elasticsearch_sql))
    # 결과 설정
    conn.setopt(pycurl.WRITEFUNCTION, result.write)
    # 실행
    conn.perform()
    res = result.getvalue().decode('UTF-8')
    # 반드시 Close 할 것!!
    result.close()
    conn.close()
    # 결과값은 문자열이므로 객체로 사용 시 변환이 필요함
    return json.loads(res)


# 페이징 처리
def make_paging(query, from_num, row_per_page):
    tmp_query = query
    tmp_query['from'] = from_num
    tmp_query['size'] = row_per_page
    tmp_query['_source'] = [
        "category",
        "path",
        "band",
        "username",
        "accessPointId",
        "mac",
        "department",
        "networkId",
        "application",
        "customer",
        "host"
    ]
    return tmp_query


# Elasticsearch에 조회 요청 : Query DSL 사용
def call_elasticsearch(code, param):
    res = search.search(
        index=get_index(code),
        body=param
    )
    return res


# Elasticsearch SQL 을 사용한 조회 테스트(Requests)
class RequestsCallTest(Resource):
    def post(self, code):
        if code is None:
            return make_error_message(1)
        parser = reqparse.RequestParser()
        parser.add_argument('pageIndex')
        parser.add_argument('pageSize')
        args = parser.parse_args()
        # SQL 생성
        sql_param = make_search_sql(code)
        # SQL -> Query DSL 변환
        # query_dsl = call_elasticsearch_sql_by_requests(sql_param)
        query_dsl = call_elasticsearch_sql_by_pycurl(sql_param)
        # 페이징 처리
        # 현재 페이지
        if 'pageIndex' not in args or not args['pageIndex']:
            page = 1
        else:
            page = int(args['pageIndex'])
        # 페이지 당 보기수
        if 'pageSize' not in args or not args['pageSize']:
            row_per_page = 20
        else:
            row_per_page = int(args['pageSize'])
        # 조회 시작 번호
        from_num = get_curr_from(page, row_per_page)
        # Query DSL에 페이징 처리
        search_query = make_paging(query_dsl, from_num, row_per_page)
        # Elasticsearch 호출
        res = call_elasticsearch(code, search_query)
        # 결과값 정리
        return make_search_result_page(res, page, from_num)


api.add_resource(KeywordSearch, '/keyword/<string:keyword_p>')
api.add_resource(TextSearch, '/text/<string:keyword_p>')
api.add_resource(KeywordSearchPaging, '/keyword/<int:page>/<string:keyword_p>')
api.add_resource(TextSearchPaging, '/text/<int:page>/<string:keyword_p>')
api.add_resource(KafkaIFTest, '/kafka_api')
api.add_resource(RequestsCallTest, '/search/<int:code>')
search = LocalProxy(get_search_conn)
producer = LocalProxy(get_kafka_conn)

if __name__ == '__main__':
    app.run()

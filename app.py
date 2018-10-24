from flask import Flask, g
from flask_restful import Resource, Api, reqparse
from werkzeug.local import LocalProxy
from elasticsearch import Elasticsearch
import logging

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


# Elasticsearch Node Connection
def get_search_conn():
    if '_elasticsearch' not in g:
        g._elasticsearch = Elasticsearch(
            [
                'https://kibanaro:kibanaro@192.168.56.101:9200',
                'https://kibanaro:kibanaro@192.168.56.102:9200',
                'https://kibanaro:kibanaro@192.168.56.103:9200'
            ],
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
def get_curr_from(page):
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
        curr_from = (20 * (page - 1))
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
                "filter": {}
            }
        },
        "sort": [],
        "_source": []
    }


# 단어 검색 샘플 API - 페이징 처리 시 scroll 사용
class KeywordSearch(Resource):
    def get(self, keyword_p):
        if keyword_p is None:
            return make_error_message(1)
        body_obj = make_basic_query()
        body_obj['query']['bool']['must'].append({
            "match": {
                "goodsname_nm": {
                    "query": keyword_p
                }
            }
        })
        del body_obj['from']
        del body_obj['size']
        del body_obj['query']['bool']['filter']
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
        body_obj = make_basic_query()
        body_obj['query']['bool']['must'].append({
            "match_phrase": {
                "goodsname_nm": {
                    "query": keyword_p
                }
            }
        })
        del body_obj['from']
        del body_obj['size']
        del body_obj['query']['bool']['filter']
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
        body_obj = make_basic_query()
        body_obj['from'] = from_num
        body_obj['query']['bool']['must'].append({
            "match": {
                "goodsname_nm": {
                    "query": keyword_p
                }
            }
        })
        del body_obj['query']['bool']['filter']
        res = search.search(
            index='search-nori-sample1',
            body=body_obj
        )
        return make_search_result_page(res, page, from_num)

    # 검색 시 조건 사용 예제 : bool, filter, sort, _source
    def post(self, page, keyword_p):
        if page is None:
            page = 1
        from_num = get_curr_from(page)
        if keyword_p is None:
            return make_error_message(1)
        parser = reqparse.RequestParser()
        parser.add_argument('best_yn')
        parser.add_argument('new_yn')
        parser.add_argument('reg_date')
        parser.add_argument('s_reg_date')
        args = parser.parse_args()
        body_obj = make_basic_query()
        body_obj['from'] = from_num
        if keyword_p is not None:
            body_obj['query']['bool']['must'].append({
                "match": {
                    "goodsname_nm": {
                        "query": keyword_p
                    }
                }
            })
        if 'best_yn' in args and args['best_yn']:
            body_obj['query']['bool']['must'].append({
                "term": {
                    "best_yn.keyword": args['best_yn']
                }
            })
        if 'new_yn' in args and args['new_yn']:
            body_obj['query']['bool']['must'].append({
                "term": {
                    "new_yn.keyword": args['new_yn']
                }
            })
        if 'reg_date' in args and args['reg_date']:
            body_obj['query']['bool']['filter']['range'] = {
                'reg_date': {
                    'gte': args['reg_date']
                }
            }
        else:
            del body_obj['query']['bool']['filter']
        if 's_reg_date' in args and args['s_reg_date']:
            body_obj['sort'].append({
                "reg_date": {
                    "order": args['s_reg_date']
                }
            })
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
        body_obj = make_basic_query()
        body_obj['from'] = from_num
        body_obj['query']['bool']['must'].append({
            "match_phrase": {
                "goodsname_nm": {
                    "query": keyword_p
                }
            }
        })
        del body_obj['query']['bool']['filter']
        res = search.search(
            index='search-nori-sample1',
            body=body_obj
        )
        return make_search_result_page(res, page, from_num)


api.add_resource(KeywordSearch, '/keyword/<string:keyword_p>')
api.add_resource(TextSearch, '/text/<string:keyword_p>')
api.add_resource(KeywordSearchPaging, '/keyword/<int:page>/<string:keyword_p>')
api.add_resource(TextSearchPaging, '/text/<int:page>/<string:keyword_p>')
search = LocalProxy(get_search_conn)

if __name__ == '__main__':
    app.run()

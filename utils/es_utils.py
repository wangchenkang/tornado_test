import logging
log = logging.getLogger("es_utils")

def filter_op(op, key, value):
    return {op: {key: value}}

def aggs_op(name, op, value, aggs = None):
    ret = { name: {op: value} }
    if aggs != None:
        ret[name]['aggs'] = aggs
    return ret

def get_field(name):
    return {"field": name}

def get_base(filter_args = [], aggs = {}, size = 100):
    return {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': filter_args
                            }
                        }
                    }
                },
            "aggs": aggs,
            "size": size
            }

def filter_course(course_id):
    return filter_op("term", "course_id", course_id)


def search(es,index,doc_type,query,search_type=None,):
    try:
        if search_type:
            res = es.search(body=query,doc_type=doc_type,index=index,search_type=search_type)  
            size = res.get("hits",{}).get("total",0)
            return size
        else:
            res = es.search(body=query,doc_type=doc_type,index=index)  
            docs = res.get("hits",{}).get("hits",[])
            return docs
    except Exception,e:
        log.error(str(e))
        if search_type:
            return 0
        else:
            return []
        

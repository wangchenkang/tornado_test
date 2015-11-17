#! -*- coding: utf-8 -*-


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
 

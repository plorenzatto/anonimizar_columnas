# coding: utf8

import json
import os
import time
import random
import socket
import hashlib
import string

import pandas as pd


SAMPLE_CONF = 'config.sample.json'
SAMPLE_DATA = 'data.sample.csv'
TEXT_TYPE = (str, unicode)
LOCAL = os.path.dirname(os.path.abspath(__file__))[:-len('/helpers')]


def clean_str(s):
    return filter(lambda c: c in string.hexdigits, s)


def hash_cache(_origin, _hash):
    cache_file = 'conversion_ids.keys'
    if isinstance(_origin, tuple):
        _origin = _origin[0]
    try:
        lines = open(cache_file, 'r').readlines()
        cache_lines = {}
        for line in lines:
            try:
                k = line.split(':')[0]
                v = line.split(':')[1]
                cache_lines.update({k: v})
            except Exception as e:
                print('Err Msg: \"{}\".'.format(e))
    except IOError:
        cache_lines = {}
    if _origin in cache_lines.keys():
        # do something
        return clean_str(cache_lines[_origin])
    else:
        # Do other thing!
        cache_lines.update({_origin: _hash})
        with open(cache_file, 'w') as cache:
            for k, v in cache_lines.items():
                cache.write('{}:{}\n'.format(k, clean_str(v)))

def anonymize_cols(_pddf=None, columns=None):
    """
    Convierte en un Hash los valores del las columnas descriptas en columns
    :param _pddf:
    :return:
    """
    if not isinstance(_pddf, pd.DataFrame):
        print('_pddf debe ser una instancia de Pandas.DataFrame')
        return None
    if not isinstance(columns, list):
        print('columns debe ser una instancia de LIST.')
        return None
    headers_count = len(columns)
    for col in columns:
        try:
            _pddf[col] = _pddf[col].apply(lambda x: generate_unique_id(x))
            headers_count -= 1
        except Exception as e:
            print(e)
            print(
                'Fallo el procesamiento de la columna:\"{}\", err: NOT-FOUND.'.format(col))
    if headers_count > 0:
        print('No fue posible procesar todas las columnas')
    return _pddf


def load_input(infn):
    """
    Carga y valida el input CSV.

    :param infn:

    Return:
        - Pandas.DataFrame: Carga exitosa.
    """
    return pd.read_csv(infn)


def generate_unique_id(data, salt=''):
    uid = hashlib.md5(data + salt).hexdigest()
    cached_hash = hash_cache(data + salt, uid)
    if cached_hash:
        return cached_hash
    else:
        return uid


def write_csv(df, output_fn=None):
    """
    Escribe un OUTPUT a un archhivo de tipo CSV.

    Args:
        - df: Pandas.DataFrame.
            - Data procesada.

        - output_fn: str o unicode
            - Nombre que recibira el archivo CSV de salida.
    Return:
        - Str: Nombre del Archivo de salida.
    """
    if not output_fn:
        output_fn = '{}.csv'.format(generate_unique_id('abc')[0:15])
    for column in df.columns:
        for idx in df[column].index:
            x = df.get_value(idx, column)
            try:
                x = unicode(x.encode('utf-8', 'ignore'),
                            errors='ignore') if type(x) == unicode else unicode(str(x), errors='ignore')
                df.set_value(idx, column, x)
            except Exception as e:
                print('encoding error: {0} {1}'.format(idx, column))
                print('Err Msg: \"{}\".'.format(e))
                df.set_value(idx, column, '')
                continue
    try:
        df.to_csv(output_fn, index=False)
        return output_fn
    except Exception as e:
        print('Ocurrio un fallo al intentar grabar el archivo {}'.format(output_fn))
        print('Err Msg: \"{}\".'.format(e))

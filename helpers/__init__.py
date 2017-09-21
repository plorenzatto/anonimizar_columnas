# coding: utf8

import functools
import hashlib
import json
import os
import string

import pandas as pd


def clean_str(s):
    return filter(lambda c: c in string.hexdigits, s)


def generate_unique_id(data, salt=''):
    return hashlib.md5(data + salt).hexdigest()


def anonymize_cols(df, columns):
    """
    Convierte en un Hash los valores del las columnas descriptas en columns
    :param _pddf:
    :return:
    """
    salt = hashlib.md5(str(random.random())).hexdigest()

    recovery_file_data = []
    for col in columns:
        recovery_file_data.append({
            "unique_values": df[col].unique().values,
            "salt": salt,
        })
        _generate_unique_id = functools.partial(generate_unique_id, salt=salt)
        df[col] = df[col].astype(str).apply(_generate_unique_id)

    cachefn = 'conversion_ids.keys'
    with open(cachefn, 'w') as f:
        json.dump(recovery_file_data, f)

    return df


def load_input(infn):
    """
    Carga y valida el input CSV.

    :param infn:

    Return:
        - Pandas.DataFrame: Carga exitosa.
    """
    return pd.read_csv(infn)


def write_csv(df, outfn):
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
    df.to_csv(outfn, encoding='utf-8')
    return outfn

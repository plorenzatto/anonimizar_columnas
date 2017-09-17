# coding: utf8
import json
import os
import time
import random
import socket
import hashlib
try:
    lib = __import__('pandas')
    globals()['pd'] = lib
except:
    print 'Este script utiliza la libreria de Python Pandas.\n' \
          'Por favor ejecuta:\n' \
          '\t sudo -H pip install pandas\n' \
          'o si se esta utilizando un VirtualEnv:\n' \
          '\t (py_venv) : pip install pandas.'
    exit(1)

SAMPLE_CONF = 'config.sample.json'
SAMPLE_DATA = 'data.sample.csv'
TEXT_TYPE = (str, unicode)
LOCAL = os.path.dirname(os.path.abspath(__file__))[:-len('/helpers')]


def clean_str(_somestr):
    allowed_chars = 'abcdef01234567890'
    cleaned_str = ''
    for c in _somestr:
        if c in allowed_chars:
            cleaned_str += c
    return cleaned_str


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
            except:
                pass
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


def resolve_kwargs(_conf, _kwargs):
    """
    Función de renderizado de KWARGS

    Args:
        - defaults:
        - _kwargs:
    Return:
        - Dict.
    """
    if not isinstance(_conf, dict) or not isinstance(_kwargs, dict):
        raise TypeError('Argumentos no validos.')
    _tmp_configs = {}
    for k, v in _conf.items():
        if k in _kwargs.keys() and isinstance(_kwargs.get(k), v.__class__):
            if isinstance(v, dict):
                _tmp_configs.update({k: resolve_kwargs(v, _kwargs.get(k))})
            else:
                _tmp_configs.update({k: _kwargs[k]})
        else:
            _tmp_configs.update({k: v})
    return _tmp_configs


def load_config(config_path=SAMPLE_CONF):
    """

    :param config_path:
    :return:
    """
    if not isinstance(config_path, TEXT_TYPE):
        print ('config_path debe ser una instancia de STR o UNICODE.')
        return None
    if config_path == SAMPLE_CONF:
        # Load Sample
        config_full_path = os.path.join(LOCAL, 'samples', config_path)
    else:
        # Load Custom Config
        config_full_path = config_path
    try:
        return json.load(open(config_full_path, 'rb'))
    except ValueError:
        print 'No es posible decodificar la configuracion: {}, no JSON parseable.'.format(config_path)
        return None
    except IOError:
        print ('No es posible localizar la configuracion: {}.'.format(config_path))
        return None


def anonymize_cols(_pddf=None, columns=None):
    """
    Convierte en un Hash los valores del las columnas descriptas en columns
    :param _pddf:
    :return:
    """
    if not isinstance(_pddf, pd.DataFrame):
        print ('_pddf debe ser una instancia de Pandas.DataFrame')
        return None
    if not isinstance(columns, list):
        print ('columns debe ser una instancia de LIST.')
        return None
    headers_count = len(columns)
    for col in columns:
        try:
            _pddf[col] = _pddf[col].apply(lambda x: generate_unique_id(x))
            headers_count -= 1
        except Exception as e:
            print (e)
            print ('Fallo el procesamiento de la columna:\"{}\", err: NOT-FOUND.'.format(col))
    if headers_count > 0:
        print ('No fue posible procesar todas las columnas')
    return _pddf


def load_input(_input_filename=None):
    """
    Carga y valida el input CSV.

    :param _input_filename:

    Return:
        - Pandas.DataFrame: Carga exitosa.
        - None: Fallo la cara del recurso.
    """
    if _input_filename == SAMPLE_DATA:
        # Load Sample
        _input_filename = os.path.join(LOCAL, 'samples', _input_filename)
    # Validar path de entrada:
    if not os.path.exists(_input_filename):
        print ('No es posible localizar el archivo: {}.'.format(os.path.basename(_input_filename)))
    with open(_input_filename, 'rb') as tmp_f:
        tmp_lines = tmp_f.readlines()
    if len(tmp_lines) > 0:
        csv_headers = tmp_lines[0].replace('\n', '').replace(' ', '').split(',')
        try:
            return pd.read_csv(_input_filename, skipinitialspace=True, usecols=csv_headers)
        except:
            pass


def generate_unique_id(*args):
    """
    source: StackOverFlow.

    """
    t = long(time.time() * 1000)
    r = long(random.random() * 100000000000000000L)
    try:
        a = socket.gethostbyname(socket.gethostname())
    except Exception as e:
        print e
        a = random.random() * 100000000000000000L
    _uid = str(t) + ' ' + str(r) + ' ' + str(a) + ' ' + str(args)
    _uid = hashlib.md5(_uid).hexdigest()
    cached_hash = hash_cache(args, _uid)
    if cached_hash:
        return cached_hash
    else:
        return _uid


def is_a_valid_conf(_conf=None):
    """
    Valida una configuracion.

    Args:
        - _conf:
            - description: configuracion provista que debe ser validada.
            - type: Dict.

    Return:
        - bool:
            - True: La config es valida.
            - False: No es valida la conf.
    """
    if not isinstance(_conf, dict):
        print ('_conf debe ser una instancia de DICT.')
        return False
    required = \
        {
            'key': 'columns',
            'type': list,
            'content': TEXT_TYPE
        }
    # exists required.key?
    if not required['key'] in _conf.keys():
        print ('{} es requerida!'.format(required['key']))
        return False
    if not isinstance(_conf[required['key']], required['type']):
        print ('{} debe contener {}'.format(required['key'], required['type']))
        return False
    if False in [isinstance(e, required['content']) for e in _conf['columns']]:
        print ('_conf[\'columns\'] debe ser una {} de {}'.format(required['type'],
                                                                 required['content']))
        return False
    return True


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
            except Exception:
                print 'encoding error: {0} {1}'.format(idx, column)
                df.set_value(idx, column, '')
                continue
    try:
        df.to_csv(output_fn, index=False)
        return output_fn
    except Exception as e:
        print ('Ocurrio un fallo al intentar grabar el archivo {}'.format(output_fn))
        print ('Err Msg: \"{}\".'.format(e))

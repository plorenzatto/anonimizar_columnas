#!/usr/bin/env python
# coding: utf-8
from helpers.helpers import *
import argparse

parser = argparse.ArgumentParser(description='Script para anonimizar clumnas en un CSV.')
parser.add_argument('-c', '--config',
                    help='Configuracion de columnas',
                    required=False,
                    default='config.sample.json')
parser.add_argument('-i', '--input',
                    help='CSV que se desea anonimizar',
                    required=False,
                    default='data.sample.csv')
parser.add_argument('-o', '--output',
                    help='Nombre del Archivo de salida',
                    required=False,
                    default='output.csv')

input_file = parser.parse_args().__dict__.get('input')
config = parser.parse_args().__dict__.get('config')
output_fn = parser.parse_args().__dict__.get('output')

if __name__ == '__main__':
    conf = load_config(config_path=config)
    if conf and is_a_valid_conf(_conf=conf):
        # Load resource
        status = write_csv(anonymize_cols(load_input(_input_filename=input_file), conf['columns']), output_fn)
        if status:
            print ('Se genero correctamente \"{}\".'.format(status))
        else:
            print 'Ocurrio un fallo a procesar el archivo: \"{}\".'.format(input_file)

    else:
        print ('Configuracion invalida, no es posible continuar.')

#!/usr/bin/env python
# coding: utf-8

import sys
import argparse

from helpers import load_input, anonymize_cols, write_csv


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Script para anonimizar clumnas en un CSV.')
    parser.add_argument('-c', '--columns' help='Columns to anonymize', required=True, nargs='*')
    parser.add_argument('-i', '--input', help='CSV que se desea anonimizar', required=True)
    parser.add_argument('-o', '--output', help='Nombre del Archivo de salida', required=True)

    args = parser.parse_args()

    conf = load_config(args.conf)
    if not is_a_valid_conf(args.conf):
        sys.exit(1)
        print ('Configuracion invalida, no es posible continuar.')

    # Load resource
    input_data = load_input(args.input)
    anonymized_data = anonymize_cols(input_data, args.columns)
    status = write_csv(anonymized_data, args.output)

    if status:
        print ('Se genero correctamente "%s".' % status)
    else:
        print ('Ocurrio un fallo a procesar el archivo: "%s".' % args.input)

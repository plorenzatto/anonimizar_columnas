Anonimizar Columnas de un Archivo `CSV`:
---

## Instalacion:
	
		git clone https://github.com/JoseSalgado1024/anonimizar_columnas.git

## Uso: 
		cd anonimizar_columnas
		python main.py --input archivo_de_entrada.csv --config configuracion.json --output archivo_de_salida.cv

## Archivo de Configuracion JSON:

_El archivo config.json, debe contar de un clave llamada `columns` y dentro de la misma, una lista de los nombres de las columnas que se desean anonimizar_

```json
{
   "columns": 
	[
	    "columna_01",
	    "columna_02"
	]
}

```

# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 21:53:29 2021

@author: B003373
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from google.cloud import bigquery
import logging


options = {'project': 'apache-beam-demo-305121',
           'runner': 'DataflowRunner',
           'staging_location': 'gs://beam_example/',
           'temp_location': 'gs://beam_example/temp/',
           'region': 'us-east1',
           }
           
pipeline_options = PipelineOptions(flags=[], **options)
pipeline = beam.Pipeline(options = pipeline_options)


query_results = (
            pipeline
            | 'Extract and transform data from source BQ' >> beam.io.ReadFromBigQuery(query='SELECT concat(cast(PD.DIV_NBR as string),"-",cast(PD.UPC as string)) AS ITEM,PS.SKU_DESC AS DESCR, 1 AS DEFAULTOUM, 0 AS PERISHABLESW, CASE  	WHEN (PS.ORDER_F = TRUE AND	 		(PS.STOP_DATE > CURRENT_DATE OR PS.STOP_DATE IS NULL ) 		) THEN "1"  	ELSE	"0"   END AS U_PROD_STATUS,PD.UPC AS U_UPC, PD.DIV_NBR AS U_DIVISION,PD.DEPT_NBR AS U_DEPT_NBR,CONCAT(cast(PD.DEPT_NBR AS string),"-",cast(PD.VND_NBR AS string)) AS U_VENDOR,PD.CLASS_NBR AS U_CLASS,PD.PID AS U_PID , PD.PID_DESC AS U_PID_DESC,PD.CLR_NBR AS U_NRF_COLOR,PD.CLR_DESC AS U_NRF_COLOR_DESC,PD.SZ_NBR AS U_NRF_SIZE,PD.SZ_DESC AS U_NRF_SIZE_DESC FROM `apache-beam-demo-305121.item_dataset.item_dim` PD JOIN `apache-beam-demo-305121.item_dataset.psku` PS 	ON PD.UPC = PS.UPC 	AND PD.DIV_NBR = PS.LOC_NBR WHERE PD.DIV_NBR = 10 AND PD.DEPT_NBR = 23',
                                     use_standard_sql=True)        
            
            )

table_spec = 'apache-beam-demo-305121:results_dataset.item_table'

table_schema = 'ITEM:STRING,DESCR:STRING,DEFAULTOUM:INTEGER,PERISHABLESW:INTEGER,U_PROD_STATUS:STRING,U_UPC:INTEGER,U_DIVISION:INTEGER,U_DEPT_NBR:INTEGER,U_VENDOR:STRING,U_CLASS:INTEGER,U_PID:STRING,U_PID_DESC:STRING,U_NRF_COLOR:INTEGER,U_NRF_COLOR_DESC:STRING,U_NRF_SIZE:INTEGER,U_NRF_SIZE_DESC:STRING'

query_results | 'Load data to BQ' >> beam.io.WriteToBigQuery(table_spec,
                                        schema=table_schema,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


logging.getLogger().setLevel(logging.INFO)

pipeline.run()


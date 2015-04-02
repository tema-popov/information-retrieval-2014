#!/bin/bash
SPARK_PATH=/Users/jambo/Applications/spark-1.2.1-bin-hadoop2.4/
${SPARK_PATH}/bin/spark-submit --master local[8] --py-files "../byweb_parser.py" collection_analysis.py


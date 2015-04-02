import os
from pyspark import SparkContext, SparkConf, StorageLevel

__author__ = 'jambo'

conf = SparkConf().setAppName("collection_analysis").setMaster("local[2]")
sc = SparkContext(conf=conf)


import sys
print sys.path
import byweb_parser

def collection_xml_file_to_raw_docs(filename):
    return (text for _, _, text in byweb_parser.iterate_documents(open(filename), filename))

def calculate_number_of_documents(documents):
    print("Documents number: {}".format(documents.count()))

def calculate_mean_document_size(documents):
    print("Documents mean size: {}".format(documents.map(len).mean()))


if __name__ == '__main__':
    dir = "/Users/jambo/projects/byweb/BY.7z"
    file_names = [os.path.join(dir, file) for file in os.listdir(dir)]
    rdd_file_names = sc.parallelize(
        file_names
    )
    documents = rdd_file_names.flatMap(collection_xml_file_to_raw_docs)
    calculate_number_of_documents(documents)
    calculate_mean_document_size(documents)


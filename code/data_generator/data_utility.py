import csv
import json
import os
from dicttoxml import dicttoxml
from xml.etree import ElementTree as Et
from kafka import KafkaProducer

def read_json_file(file_name):
    with open(file_name) as json_file:
        data = json.load(json_file)
    return data


def get_project_path():
    return os.getcwd().rsplit('/', 1)[0]


def write_in_file(input_d, file_name, file_mode):
    with open(file_name, file_mode) as t:
        t.write(input_d)


def lis_of_map_to_json(input_d, file_name, file_mode):
    #with open(file_name, file_mode) as fw:
    producer = KafkaProducer(bootstrap_servers='20.0.31.221:9092')
    for json_record in input_d:
        record_write = json.dumps(json_record)
        #producer = KafkaProducer(bootstrap_servers='20.0.31.221:9092')
        producer.send('jsonvideoanalytics',record_write)
        #fw.write(record_write)
        #fw.write("\n")


def lis_of_map_to_csv(input_d, file_name, file_mode):
    #print(input_d)
    write_in_csv(input_d, file_name, file_mode)


csv_counter = 0


def write_in_csv(input_d, file_name, file_mode):
    keys = input_d[0].keys()
    with open(file_name, file_mode) as output_file:
        global csv_counter
        csv_counter += 1
        if csv_counter > 1:
            dict_writer = csv.DictWriter(output_file, keys)
        else:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
        dict_writer.writerows(input_d)


def lis_of_map_to_xml(input_d, file_name, file_mode):
    #print(input_d)
    write_in_xml(input_d, file_name, file_mode)


def write_in_xml(input_d, file_name, file_mode):
    list_of_str = []
    for sd in input_d:
        xml_data = dicttoxml(sd, custom_root='record', attr_type=False)
        tree = Et.XML(xml_data)
        list_of_str.append(str(Et.tostring(tree).decode('utf-8')))
    with open(file_name, file_mode) as f:
        for item in list_of_str:
            f.write("%s\n" % str(item))


def create_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

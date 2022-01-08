import os
import random
import string
from datetime import date
from datetime import datetime
from datetime import timedelta
from random import randrange

#from data_generator import data_csv
#from data_generator import data_json
import data_utility as util
#from data_generator import data_xml

meta_map = {}
var_map = {}
global_counter = {}
list_of_dict = []
count = random.randint(0, 1000)


def read_inp_file():
    inp_file_path = util.get_project_path() + '/data_generator/resource/input_info.json'
    return util.read_json_file(inp_file_path)  # type: object


def prepare_meta(inp_data):
    meta = inp_data['meta_info']
    formats = set(meta['format_of_gen_file'])

    for file_format in formats:
        output_folder_path = os.path.join(util.get_project_path(), 'out', meta['destination_folder_path'],
                                          str(date.today()), str(count),
                                          file_format)
        print('output folder path' + output_folder_path)
        util.create_dir(output_folder_path)
        op_file_path = os.path.join(output_folder_path, meta['output_file_name']) + "." + file_format
        print(op_file_path)
        meta_map[file_format] = op_file_path


def prepare_var_map(inp_data):
    data_set = inp_data['data_info']

    for data in data_set:
        var_map[data['var_name']] = data


def prepare_data(inp_data):
    meta = inp_data['meta_info']
    no_of_rows = meta['no_of_rows']

    #json_obj = data_json.JSon()
    #csv_obj = data_csv.CSV()
    #xml_obj = data_xml.XML()
    csv_gen = None
    json_gen = None
    xml_gen = None

    for key, value in meta_map.items():
        if str(key).lower() == 'json':
            json_gen = value
            print(json_gen)
        if str(key).lower() == 'csv':
            csv_gen = value
        if str(key).lower() == 'xml':
            xml_gen = value

    counter = 0
    for i in range(no_of_rows):
        data_map = {}
        for key, value in var_map.items():
            data_map[key] = check(value, value['var_type'])
        list_of_dict.append(data_map)
        counter += 1

        if counter == 999:
            print(list_of_dict)
            if json_gen is not None:
                util.lis_of_map_to_json(list_of_dict, json_gen, "a")
            if csv_gen is not None:
                util.lis_of_map_to_csv(list_of_dict, csv_gen, "a")
            if xml_gen is not None:
                util.lis_of_map_to_xml(list_of_dict, xml_gen, "a")
            counter = 0
            del list_of_dict[:]

    if counter > 0:
        print('less than 1000')
        print(list_of_dict)
        if json_gen is not None:
            util.lis_of_map_to_json(list_of_dict, json_gen, "a")
        if csv_gen is not None:
            util.lis_of_map_to_csv(list_of_dict, csv_gen, "a")
        if xml_gen is not None:
            util.lis_of_map_to_xml(list_of_dict, xml_gen, "a")


def check(data, var_type):
    if var_type == "str":
        return prepare_str_data(data)
    elif var_type == "int":
        return prepare_int_data(data)
    elif var_type == "float":
        return prepare_float_data(data)
    elif var_type == "bool":
        return prepare_bool_data(data)
    elif var_type == "date":
        return prepare_date_data(data)
    elif var_type == "datetime":
        return prepare_datetime_data(data)
    else:
        return None


def prepare_datetime_data(data):
    print('inside prepare_datetime_data')
    var_constraint = data['var_constraint']
    def_val = var_constraint['default']
    date_time_format = var_constraint['date_time_format']
    start_date_time_str = var_constraint['start_date_time']
    end_date_time_str = var_constraint['end_date_time']
    start_date_time = datetime.strptime(start_date_time_str, date_time_format)
    end_date_time = datetime.strptime(end_date_time_str, date_time_format)
    temp_date = None
    if len(def_val) == 0:
        temp_date = random_date(start_date_time, end_date_time)
    elif start_date_time_str == '' and end_date_time_str == '':
        temp_date = datetime.now()
    else:
        random.choice(def_val)
    return str(temp_date.strftime(date_time_format))


def prepare_date_data(data):
    print('inside prepare_date_data')
    var_constraint = data['var_constraint']
    def_val = var_constraint['default']
    date_format = var_constraint['date_format']
    start_date_str = var_constraint['start_date']
    end_date_str = var_constraint['end_date']
    start_date = datetime.strptime(start_date_str, date_format)
    end_date = datetime.strptime(end_date_str, date_format)
    temp_date = None
    if len(def_val) == 0:
        temp_date = random_date(start_date, end_date).date()
    elif start_date_str == '' and end_date == '':
        temp_date = date.today()
    else:
        random.choice(def_val)
    return str(temp_date.strftime(date_format))


def prepare_bool_data(data):
    print('inside prepare_bool_data')
    var_constraint = data['var_constraint']
    def_val = var_constraint['default']
    if len(def_val) == 0:
        temp_bool = bool(random.getrandbits(1))
    else:
        temp_bool = bool(random.choice(def_val))
    return temp_bool


def prepare_float_data(data):
    print('inside prepare_float_data')
    var_constraint = data['var_constraint']
    def_val = var_constraint['default']
    start_range = var_constraint['start_range']
    end_range = var_constraint['end_range']
    precision = var_constraint['precision']
    if len(def_val) == 0:
        temp_float = round(random.uniform(start_range, end_range), precision)
    else:
        temp_float = float(random.choice(def_val))
    return temp_float


def prepare_int_data(data):
    print('inside prepare_int_data')
    var_constraint = data['var_constraint']
    var_name = data['var_name']
    def_val = var_constraint['default']
    start_range = var_constraint['start_range']
    end_range = var_constraint['end_range']
    incremental_by = var_constraint['incremental_by']
    temp_int = 0
    if len(def_val) == 0:
        if incremental_by == 0:
            return random.randint(start_range, end_range)
        if var_name in global_counter:
            temp_int = global_counter[var_name]
        if temp_int == 0:
            temp_int = start_range + incremental_by
        elif temp_int >= end_range:
            temp_int = 0
        else:
            temp_int += incremental_by
    else:
        temp_int = random.choice(def_val)
    global_counter[var_name] = temp_int
    return temp_int


def prepare_str_data(data):
    print('inside prepare_str_data')
    var_constraint = data['var_constraint']
    def_val = var_constraint['default']
    allow_null = var_constraint['allow_null']
    if len(def_val) == 0:
        max_len = var_constraint['max_len']
        min_len = var_constraint['min_len']
        temp_str = ''.join(random.choice(string.ascii_lowercase) for _ in range(random.randint(min_len, max_len)))
        prefix = var_constraint['prefix']
        suffix = var_constraint['suffix']
        temp_str = prefix + '' + temp_str + '' + suffix
    else:
        temp_str = random.choice(def_val)
    if allow_null:
        temp = random.randint(0, 10)
        if temp == 6:
            temp_str = None
    print("prepared string is :" + str(temp_str))
    return temp_str


def random_date(start, end):
    """
    This function will return a random datetime between two datetime objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


def execute():

    input_data = read_inp_file()
    prepare_meta(input_data)
    prepare_var_map(input_data)
    prepare_data(input_data)


if __name__ == "__main__":
    """ This is the starting method for data generator.
    We can define the all parameter in resouce/input_info.json file.
    This application can generate random data in xml,csv,json format. 
    """
    execute()

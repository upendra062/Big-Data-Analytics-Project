from data_generator.data_base import GenBase
from data_generator import data_utility as util


class XML(GenBase):
    def write_file(self, input_d, file_name, file_mode):
        return util.lis_of_map_to_xml(input_d, file_name, file_mode)

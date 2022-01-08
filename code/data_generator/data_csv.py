from data_base import GenBase
import data_utility as util


class CSV(GenBase):
    def write_file(self, input_d, file_name, file_mode):
        return util.lis_of_map_to_csv(input_d, file_name, file_mode)

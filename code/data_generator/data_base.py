import abc


class GenBase(abc.ABC):

    @abc.abstractmethod
    def write_file(self, input_d, file_name, file_mode):
        """Retrieve data from the input source and return an object."""
        return

import threading


class ShardedFile:
    """
    Class to represent a chunk of a file
    Chunks are stored across nodes in the network
    """

    def __init__(self, chunk_id=None, data=None):
        self.chunk_id = chunk_id
        self.data = data

    def serialize(self):
        return self.data

    @classmethod
    def deserialize(cls, data):
        return cls(data=data)


class File:
    """
    Class to represent a file
    """

    def __init__(self, file_name, file_content):
        self.file_name = file_name
        self.file_content = file_content
        self.file_version = 0

    def serialize(self):
        return self.file_content


class FileStore:
    """
    Class to represent storage of files
    Has a map <filename: list(File)>
    """

    def __init__(self):
        self.file_map = {}

    def add_file(self, file_name, file_content):
        """
        Add a file to the file store
        """
        if file_name not in self.file_map:
            self.file_map[file_name] = []
        self.file_map[file_name].append(File(file_name, file_content))

    def get_file(self, file_name):
        """
        Get a file from the file store
        """
        if file_name not in self.file_map:
            return None
        return self.file_map[file_name][-1]

    def get_file_version(self, file_name):
        """
        Get the version of a file
        """
        if file_name not in self.file_map:
            return None
        return self.file_map[file_name][-1].file_version

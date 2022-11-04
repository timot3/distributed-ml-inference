from typing import List

from FileStore.types import MAX_NUM_VERSIONS


class File:
    """
    Class to represent a file
    """

    def __init__(self, file_name: str, file_content: bytes):
        self.file_name = file_name
        self.file_content = file_content

    def serialize(self):
        return self.file_content


class FileStore:
    """
    Class to represent storage of files
    Has a map <filename: list(File)>
    """

    def __init__(self):
        self.file_map = {}

    def put_file(self, file_name, file_content):
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

        # get the latest version of the file
        return self.file_map[file_name][-1]

    def get_file_version(self, file_name) -> File:
        """
        Get the version of a file
        """
        if file_name not in self.file_map:
            return None
        return self.file_map[file_name][-1].file_version

    def get_file_versions(self, file_name, num_versions) -> List[File]:
        """
        Get the last num_versions vesions of the file
        :param file_name: The file name to get
        :param num_versions: The number of versions to get
        :return: A list of (up to) the last 5 versions of the file
        """
        if file_name not in self.file_map:
            return []
        # cap the number of versions at MAX_NUM_VERSIONS
        if num_versions > MAX_NUM_VERSIONS:
            num_versions = MAX_NUM_VERSIONS
        return self.file_map[file_name][-num_versions:]

    def delete_file(self, file_name) -> List[File]:
        """
        Delete a file from the file store
        :param file_name: The file name to delete
        :return: A list of the versions of the file that were deleted
        """
        if file_name not in self.file_map:
            return []
        return self.file_map.pop(file_name)

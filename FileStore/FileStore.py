import struct
from typing import List, Optional, Union

from FileStore.filetypes import MAX_NUM_VERSIONS


class File:
    """
    Class to represent a file
    """

    def __init__(
        self,
        file_name: str,
        file_content: Union[bytes, bytearray],
        filesize: int = 0,
        version: int = 0,
    ):
        self.file_name = file_name
        self.file_content = file_content
        if filesize == 0:
            self.file_size = len(file_content)
        else:
            self.file_size = filesize
        self.version = version

    def serialize(self):

        return self.file_content

    @classmethod
    def deserialize(cls, data: bytes):
        pass

    def ls_serialize(self):
        # return bytes of the following form:
        # 32 bytes for the filename
        # 4 bytes for the version
        # 4 bytes for the length of the data in bytes
        # all of this is separated by a colon

        # use struct.pack to pack the data into bytes
        name_bytes = struct.pack(">32s", self.file_name.encode("utf-8"))
        version_bytes = struct.pack(">I", self.version)
        size_bytes = struct.pack(">I", self.file_size)
        ret = name_bytes + b":" + version_bytes + b":" + size_bytes
        return ret

    @classmethod
    def ls_deserialize(cls, data: Union[bytes, bytearray]):
        # use struct.unpack to unpack the data from bytes
        name, version, size = data.split(b":")
        name = struct.unpack(">32s", name)[0].rstrip(b"\x00").decode("utf-8")
        # the next 4 bytes are the version
        version = struct.unpack(">I", version)[0]
        # the next 4 bytes are the size of the file
        size = struct.unpack(">I", size)[0]

        return cls(name, b"", filesize=int(size), version=int(version))

    def __str__(self):
        text = (
            f"FileName: {self.file_name}, Version: {self.version}, Size: {self.file_size}"
        )
        if len(self.file_content) > 0:
            # add the first 10 bytes
            text += f", Content: {self.file_content[:10]}"
        return text


class FileStore:
    """
    Class to represent storage of files
    Has a map <filename: list(File)>
    """

    def __init__(self):
        self.file_map = {}

    def put_file(self, file_name: str, file_content: Union[bytes, bytearray]):
        """
        Add a file to the file store
        """
        if file_name not in self.file_map:
            self.file_map[file_name] = []

        # increment the version number by 1
        file_version = self.get_file_version(file_name) + 1
        print(file_version)
        self.file_map[file_name].append(
            File(file_name, file_content, version=file_version)
        )

    def get_file(self, file_name) -> Optional[File]:
        """
        Get a file from the file store
        """
        if file_name not in self.file_map:
            return None

        # get the latest version of the file
        return self.file_map[file_name][-1]

    def get_file_version(self, file_name) -> int:
        """
        Get the version of a file
        """
        if file_name not in self.file_map or len(self.file_map[file_name]) == 0:
            return -1  # file does not exist

        return self.file_map[file_name][-1].version

    def has_file_version(self, file_name, file_version) -> bool:
        """
        Check if a file has a particular version
        :param file_name: The file name to check
        :param file_version: The version to check. If -1, check if the file exists
        :return: True if the file has the version, False otherwise
        """
        if file_version < 0:
            return file_name in self.file_map

        if file_name not in self.file_map:
            return False

        for file in self.file_map[file_name]:
            if file.version == file_version:
                return True
        return False

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

    def delete_file(self, file_name) -> Optional[File]:
        """
        Delete a file from the file store
        :param file_name: The file name to delete
        :return: A list of the versions of the file that were deleted
        """
        if file_name not in self.file_map:
            return None
        return self.file_map.pop(file_name)

    def get_latest_versions(self) -> List[File]:
        """
        Get the latest version of all files
        :return: A list of the latest version of all files
        """
        return [self.file_map[file_name][-1] for file_name in self.file_map]

    def get_file_names(self) -> List[str]:
        """
        Get a list of all the file names
        :return: A list of all the file names
        """
        return list(self.file_map.keys())

    def __str__(self):
        text = ", ".join([file_name for file_name in self.file_map])
        return text

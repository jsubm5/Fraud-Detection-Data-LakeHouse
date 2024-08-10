import os
import glob
from typing import List, Union
from includes.modules.file_utils.base import Handler 
class DirHandler(Handler):
    def get_files_with_pattern(self, pattern: str) -> List[str]:
        files = []

        for path in self.path:
            search_pattern = os.path.join(path, pattern)
            files.extend(glob.glob(search_pattern))
        
        normalized_files = [file for file in files if self._matches_pattern(file, pattern)]
        return normalized_files

    def _matches_pattern(self, file: str, pattern: str) -> bool:
        """
        Check if the file matches the given pattern in a case-insensitive manner.
        """
        pattern_parts = pattern.lower().split('*')
        file_name = os.path.basename(file).lower()
        return all(part in file_name for part in pattern_parts)

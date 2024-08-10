from includes.modules.file_utils.base import Handler 

class FileHandler(Handler):
    def read_files(self)->list:
        paths=self.path
        content = []
        for path in paths:
            content.append(self._read_file(path))
        return content
        
    def _read_file(self, path)->str:
        with open(path, 'r') as file:
            return file.read().strip()
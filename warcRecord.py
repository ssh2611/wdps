from io import StringIO
import re

class WarcRecord:
    def __init__(self, record):
        self.id = None
        self.payload = None
        self.broken = None
        self._parse(record)

    def _parse(self, record):
        capture = re.search('WARC-Record-ID: <(\S*)>', record, re.IGNORECASE)
        if capture:
            self.id = capture.group(1)
            self.broken = False
            split = record.split("HTTP")
            if len(split) > 1:
                self.payload = split[1]
        else: 
            self.broken = True

    

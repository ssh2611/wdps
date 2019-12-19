from io import StringIO
import re

class WarcRecord:
    def __init__(self, record):
        self.id = None
        self.payload = None
        self.broken = None
        self._parse(record)

    def _parse(self, record):
        # print(record)
        # capture = re.search('WARC-Record-ID: <(\S*)>', record, re.IGNORECASE)
        capture = re.search('WARC-TREC-ID: (\S*)', record, re.IGNORECASE)
        if capture:
            self.id = capture.group(1)
            self.broken = False
            split = record.split("<html ")
            if len(split) > 1:
                buffer = StringIO(("<html " + split[1]).strip())
                while True:
                    if buffer.readline().strip()== '':
                        break
                self.payload = buffer.read().strip()
               # print(self.payload)
        else: 
            self.broken = True

    

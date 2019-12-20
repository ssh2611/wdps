from io import StringIO
import re
import logging

#logger = logging.getLogger('py4j')
#logger.info("My test info statement")


class WarcRecord:
    def __init__(self, record):
        self.id = None
        self.payload = "Default"
        self.broken = True
        self._parse(record)

    def _parse(self, record):
        # print(record)
        # capture = re.search('WARC-Record-ID: <(\S*)>', record, re.IGNORECASE)
        capture = re.search('WARC-TREC-ID: (\S*)WARC', record, re.IGNORECASE)
        #raise Exception("RECORD: %s \n CAPTURE %s" % (record, capture))
        if capture:
            #raise Exception("RECORD: %s \n CAPTURE %s" % (record, capture))
            self.id = capture.group(1)
            self.payload = record.split("html", 2)[1]
            #split = record.split("html", 2)
            #if len(split) > 1:
            #    self.payload = split[1]
            #    self.broken = False
            #    buffer = StringIO(("html" + split[1]).strip())
            #    while True:
            #        if buffer.readline().strip() == '':
            #            break
            #    self.payload = buffer.read().strip()
               # print(self.payload)
        else:
            self.broken = True

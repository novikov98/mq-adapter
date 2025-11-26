import logging
import re


class MaskTokenFilter(logging.Filter):
    TOKEN_RE = re.compile(r"/rest/\d+/([A-Za-z0-9]+)/")

    def filter(self, record):
        new_args = []

        for arg in record.args:
            if isinstance(arg, (int, float)):
                new_args.append(arg)
                continue

            # приводим к строке (logging сам бы сделал это позже)
            s = str(arg)

            # маскируем токен в URL
            s = self.TOKEN_RE.sub(r"/rest/*******/", s)

            new_args.append(s)

        record.args = tuple(new_args)
        return True

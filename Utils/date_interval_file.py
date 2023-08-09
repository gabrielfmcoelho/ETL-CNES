import re

class Date_interval_file:
    def __init__(self, mode, items, targets, start_date, end_date):
        self.mode = mode
        self.items = items
        self.targets = targets
        self.start_date = start_date
        self.end_date = end_date
        self.query_targets = []

    def verify_files(self):
        for target in self.targets:

            for item in self.items:
                if regex_string := re.search(target, item):
                    if self.mode==3:
                        self.query_targets.append(item)
                        self.query_targets.sort()
                    elif self.mode==2:
                        date_position = int(regex_string.span()[1])
                        date = int(item[date_position:(date_position+6)])
                        if date >= self.start_date and date <= self.end_date:
                            self.query_targets.append(item)
                            self.query_targets.sort()

        return self.query_targets
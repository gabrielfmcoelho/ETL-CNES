import re

class Most_recent_file:
    def __init__(self, items, targets):
        self.items = items
        self.targets = targets 
        self.query_targets = []

    def verify_files(self):
        for target in self.targets:
            temp_date = 000000

            for item in self.items:
                if regex_string := re.search(target, item):
                    date_position = int(regex_string.span()[1])
                    date = int(item[date_position:(date_position+6)])

                    if date > temp_date:
                        target_most_recent = item
                        temp_date = date

            self.query_targets.append(target_most_recent)
        return self.query_targets
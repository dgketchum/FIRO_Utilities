class Dict:

    def __init__(self):
        None

    def csv_to_dict(self, source_file, headers=None,  has_header=True):
        import csv
        from collections import defaultdict
        project = defaultdict(dict)
        with open(source_file, 'rb') as fp:
            if has_header:
                fp.next()
            reader = csv.DictReader(fp, fieldnames=headers, dialect='excel',
                                    skipinitialspace=True)
            for rowdict in reader:
                if None in rowdict:
                    del rowdict[None]
                station_ID = rowdict.pop("StationID")
                name = rowdict.pop("Name")
                project[station_ID][name] = rowdict
        return dict(project)
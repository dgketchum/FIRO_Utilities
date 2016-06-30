class readGauges:

    def __init__(self):
        None

    def read_table_rows(self, directory, filename):
        import os
        fid = open(os.sep.join([directory, filename]))
        print fid
        lines = fid.readlines()
        fid.close()
        rows = [line.split(',') for line in lines]
        return rows

    def fill_hours(self, line):
        from datetime import datetime, timedelta
        for xx in range(0, 24):
            if line[xx + 3] not in ['m', 'm\n']:
                day = datetime.strptime(line[2], '%Y%m%d')
                day_hour = day + timedelta(hours=xx)
                line_data = ([day_hour, line[xx + 3]])
                return line_data

    def read_usace_gauge(self, directory, file_names):
        q_recs = []
        s_recs = []
        out_q = []
        stor = []

        for filename in file_names:

            if filename.endswith('.txt') and filename != 'readme.txt':

                rows = readGauges.read_table_rows(directory, filename)

                for line in rows:

                    # '20'  discharge, '76' reservoir inflow
                    if line[0] in ['CLV', 'COY'] and line[1] in ['20', '76']:
                        q_recs.append(readGauges.fill_hours)

                    # '1' is river stage, '6' reservoir stage
                    if line[0] in ['CLV', 'COY'] and line[1] in ['1', '6']:
                        s_recs.append(readGauges.fill_hours)

                    if line[0] == 'COY' and line[1] == '15':  # '15' is res. storage
                        stor.append(readGauges.fill_hours)

                    if line[0] == 'COY' and line[1] == '23':  # '23' is res. outflow
                        out_q.append(readGauges.fill_hours)

    def __str__(self):
        return 'Calculator class'
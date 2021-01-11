import pandas as pd
from io import StringIO
import time
import json
import re
from yandex_metrika import YandexMetrika

if __name__ == "__main__":

    #  Read token and Yandex Metrika counter ID

    with open('user_data\\user_data.txt') as json_file:
        data = json.load(json_file)
        TOKEN = data['token']
        COUNTER_ID = data['counter_id']

    #  Initialize YandexMetrika

    ym = YandexMetrika(TOKEN)

    # Configure fields for Yandex Metrika report

    ym_fields = ["ym:s:visitID",
                 "ym:s:clientID",
                 "ym:s:dateTime",
                 "ym:s:UTMSource",
                 "ym:s:UTMMedium",
                 ]
    start_date = '2020-06-27'
    end_date = '2020-06-27'
    ym_source = 'visits'

    # Check report building possibility

    possible = ym.get_report_possibility(counter_id=COUNTER_ID,
                                         date_from=start_date,
                                         date_to=end_date,
                                         source=ym_source,
                                         fields=ym_fields)

    if not possible.get('possible'):
        print("Report with following params couldn't be build")
    else:
        print("Report with chosen params could be build")

        #  So let's build that report

        report = ym.get_report(counter_id=COUNTER_ID,
                               date_from=start_date,
                               date_to=end_date,
                               source=ym_source,
                               fields=ym_fields)

        # Extracting report ID and status for further downloading

        report_id = report.get('log_request').get('request_id')
        status = report.get('log_request').get('status')

        #  Waiting while status is not 'processed'

        while status == 'created':
            sleep_time = 30
            status = ym.get_status(counter_id=COUNTER_ID,
                                   request_id=report_id).get('log_request').get('status')

            output = """Report {report_id} is still have '{status}' status.
            Waiting for {sleep_time} seconds before checking status again""" \
                .format(report_id=report_id, status=status, sleep_time=sleep_time)

            print(re.sub(r'\s+', ' ', output))

            time.sleep(sleep_time)
        else:
            parts = ym.get_status(counter_id=COUNTER_ID,
                                  request_id=report_id).get('log_request').get('parts')

            output = """Report {report_id} is '{status}'. You can download it.
            Report was divided into following parts: {parts}""" \
                .format(report_id=report_id, status=status, parts=parts)

            print(re.sub(r'\s+', ' ', output))

        #  If report was divided into several parts, we need final DataFrame to collect all the parts

        report_df = pd.DataFrame()

        # So let's collect all the data from each part and append it do final DataFrame

        for part in parts:
            part_id = part.get('part_number')
            part_data = ym.download_report(counter_id=COUNTER_ID,
                                           request_id=report_id,
                                           part_number=part_id).content

            part_df = pd.read_csv(StringIO(str(part_data, encoding='utf8')), sep='\t')

            report_df = report_df.append(part_df)
            print("Part {part_id} was appended into final DataFrame"
                  .format(part_id=part_id))

        print(report_df.head())

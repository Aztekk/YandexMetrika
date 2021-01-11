from urllib.parse import urlencode
import requests
import json

HOST = 'https://api-metrika.yandex.ru'


class YandexMetrika(object):
    """
    Yandex.Metrika class
    """
    token = None

    def __init__(self, token=None):
        """
        Constructor of the object

        :param token: Oauth-token
        :type token: str

        :return: nothing
        """

        self.token = token

        if not token:
            print('Недействительный OAuth-токен')

    def get_header(self):
        """
        Generates header for http-requests

        :rtype: dict
        :return: header object
        """

        return {
            "Authorization": "OAuth {}".format(self.token)
        }

    def get_report_possibility(self, counter_id, date_from, date_to, source, fields):
        """
        Check of it possible to get report with chosen params
        :param counter_id: Yandex.Metrika counter ID
        :param date_from: start date of statistics collection. Format "YYYY-MM-DD"
        :param date_to: end date of statistics collection. Format "YYYY-MM-DD"
        :param source: "hits" or "visits" - data aggregation you need
        :param fields: list of string with needed fields.
        :return: json object with the information about possibility of collection report with chosen params
        """

        url_params = urlencode(
            [
                ('date1', date_from),
                ('date2', date_to),
                ('source', source),
                ('fields', ','.join(fields))
            ]
        )

        headers = self.get_header()

        url = '{host}/management/v1/counter/{counter_id}/logrequests/evaluate' \
            .format(host=HOST, counter_id=counter_id)

        r = requests.get(url, headers=headers, params=url_params)

        if r.status_code == 200:
            return json.loads(r.text)['log_request_evaluation']
        else:
            raise ValueError(r)

    def get_report(self, counter_id, date_from, date_to, source, fields):
        """
        If collection report is possible, you can collect it
        :param counter_id: Yandex.Metrika counter ID
        :param date_from: start date of statistics collection. Format "YYYY-MM-DD"
        :param date_to: end date of statistics collection. Format "YYYY-MM-DD"
        :param source: "hits" or "visits" - data aggregation you need
        :param fields: list of string with needed fields.
        :return: json object with the information about report. You need "request_id" and "status"
        """

        url_params = urlencode(
            [
                ('date1', date_from),
                ('date2', date_to),
                ('source', source),
                ('fields', ','.join(fields))
            ]
        )

        headers = self.get_header()

        url = '{host}/management/v1/counter/{counter_id}/logrequests' \
            .format(host=HOST, counter_id=counter_id)

        r = requests.post(url, headers=headers, params=url_params)

        if r.status_code == 200:
            return json.loads(r.text)
        else:
            raise ValueError(r)

    def get_status(self, counter_id, request_id):
        """
        Returns status of report

        :param counter_id: Yandex.Metrika counter ID
        :param request_id: ID of request returned by get_report function
        :return: status of the chosen report ID
        """

        headers = self.get_header()

        url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}' \
            .format(host=HOST, counter_id=counter_id, request_id=request_id)

        r = requests.get(url, headers=headers)

        if r.status_code == 200:
            return json.loads(r.text)
        else:
            raise ValueError(r)

    def download_report(self, counter_id, request_id, part_number):
        """
        Function for downloading processed report. Use it after report status becomes "processed".

        :param counter_id: Yandex.Metrika counter ID
        :param request_id: ID of request returned by get_report function
        :param part_number: number of the part, if report was divided into several parts
        :return: .tsv string with report data
        """

        headers = self.get_header()

        url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part_id}/download' \
            .format(host=HOST, counter_id=counter_id,
                    request_id=request_id, part_id=part_number)

        report_tsv = requests.get(url, headers=headers)

        return report_tsv

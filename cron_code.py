import time
import schedule
import pymssql
import requests
from pymongo import MongoClient
import datetime

class CodReturnRefundUTR:
    def __init__(self, debug=True,test_val=0):
        self.DEBUG = debug
        self.test_val = test_val
        self.dbalias = 'ssspl_webuser'
        self.TEST = False
        self.LOOP_RUNNING = False
        self.statement_initiate_url_local = "http://0.0.0.0:8000/statement/initiate"
        self.statement_retrieval_url_local = "http://0.0.0.0:8000/statement/retrieval"

        if self.DEBUG:
            self.schedule_job()
        else:
            schedule.every(60).minutes.do(self.schedule_job)
            while True:
                schedule.run_pending()
                time.sleep(5)

    def schedule_job(self):
        if not self.LOOP_RUNNING:
            self.LOOP_RUNNING = True
            try:
                self.start()
            except Exception as ex:
                print("exception:", ex)

            self.LOOP_RUNNING = False

    def start(self):
        # start_date = self.recent_cron_execution_date()
        # print(start_date, type(start_date))
        start_date = "2022-12-2"
        if self.TEST:
            mapped_ids = [['CTDBBB37DC', 'citiCTDBBB37DC'], ['CT80A9463F', 'CITICT80A9463F'],
                          ['CT81AA73A4', 'CITICT81AA73A4'], ['CTE9684484', 'CITICTE9684484'],
                          ['CTD414D3F2', 'CITICTD414D3F2'], ['CT53627F4E', 'CITICT53627F4E'],
                          ['CT53F85808', 'CITICT53F85808'], ['CT42BA9996', 'CITICT42BA9996'],
                          ['CTF001E661', 'CITICTF001E661'], ['CT5F7BA08A', 'CITICT5F7BA08A'],
                          ['CT51D3B5BC', 'CITICT51D3B5BC'], ['CTB5628597', 'CITICTB5628597'],
                          ['CT6C63BE19', 'CITICT6C63BE19'], ['CT30A0E39A', 'CITICT30A0E39A'],
                          ['CT1C550029', 'CITICT1C550029'], ['CT24B9A56E', 'CITICT24B9A56E'],
                          ['CT8A2A574A', 'CITICT8A2A574A'], ['CT6CBD0F26', 'CRON_test' + str(self.test_val)]]
        else:
            citi_statement = self.fetch_citi_statement(start_date)
            # mapped_ids = self.utr_extract(CITI_STATEMENT)
            mapped_ids = self.utr_extract(citi_statement)
            print("length:", len(mapped_ids))
            mapped_ids.append(['CT6CBD0F26', 'CRON_test' + str(self.test_val)])

        server = "11.19.40.18"
        user = "FKPranay"
        password = "FK@@$h(7c^7*G%Qa"

        conn = pymssql.connect(server, user, password, "SSSPL_NEW")
        cursor = conn.cursor()



        for item in mapped_ids:
            if item[0] and item[1]:
                query = "EXECUTE [PaymentManagement].[uspSaveRefundRRNNumber] @bankReferenceId = '" + str(item[0]) +\
                        "', @RRN = '" + str(item[1]) + "'"
                # vals = []
                # db_connection = dbcon.connection(self.dbalias)
                # cursor = db_connection.cursor()
                # cursor.execute(query, tuple(vals))
                # cursor.close()
                # db_connection.close()
                cursor.execute(query)
                conn.commit()
        cursor.close()
        conn.close()

    def recent_cron_execution_date(self):
        mongo_client = MongoClient("11.19.40.33:27017")
        db_sspl = mongo_client['sspl']
        collection = db_sspl["CronLog"]
        doc = collection.find({"Key": "CitiBankStatementCronExecutedDate"}).limit(1)

        start_date = doc[0]['UpdatedDate']
        end_date = datetime.datetime.utcnow()

        result = collection.update({'Key': "CitiBankStatementCronExecutedDate"}, {'$set': {'UpdatedDate': end_date}})
        return start_date

    def fetch_citi_statement(self, start_date):

        initiate_headers = {"Content-Type": "application/json"}
        retrieval_headers = {"Content-Type": "application/json"}
        end_date = "2022-11-30"
        initiate_data = {
            "data": {
                "fromDate": start_date,
                "toDate": end_date
            }
        }
        retrieval_data = {
            "data": {
                "statementId": "111111111"
            }
        }
        initiate_request = requests.post(url=self.statement_initiate_url_local, headers=initiate_headers,
                                         json=initiate_data)
        initiate_response = initiate_request.json()
        print(initiate_response)

        retrieval_response = requests.post(url=self.statement_retrieval_url_local, headers=retrieval_headers,
                                           json=retrieval_data)
        response = retrieval_response.json()
        print(response[0])
        print(type(response[0]))
        return response[0]

    def utr_extract(self, statement_lines):
        #statement = statement_lines.read()
        statement = statement_lines.split('\n')
        response = []

        # utr_list = []
        # statement_lines = []
        # refund_list = []

        # going through all the transactions
        for start in range(len(statement)):
            if statement[start][:4] == ":61:":
                mapped_ids = [None, None]  # [refund_id, UTR_number]
                end_61 = start + 1
                while end_61 < len(statement) and (
                        statement[end_61][:4] != ":86:" and statement[end_61][:5] != ":62F:" and
                        statement[end_61][:4] != ":64:" and
                        statement[end_61][:4] != ":61:"):
                    end_61 += 1

                transaction_61 = ''
                for i in range(start, end_61):
                    transaction_61 += statement[i]  # we have the :61: field of the statement

                refund_id = self.get_refund_id(transaction_61)
                mapped_ids[0] = refund_id

                if statement[end_61][:4] == ":86:":
                    end_86 = start + 1
                    while end_86 < len(statement) and (
                            statement[end_86][:4] != ":61:" and statement[end_86][:5] != ":62F:" and
                            statement[end_86][:4] != ":64:"):
                        end_86 += 1

                    transaction_86 = ''
                    for i in range(end_61, end_86):
                        transaction_86 += statement[i]  # we have the :86: field of the statement

                    utr = self.get_utr(transaction_86)
                    mapped_ids[1] = utr

                #     if utr:
                #         utr_list.append(utr)
                #     if refund_id:
                #         refund_list.append(refund_id)
                #     statement_lines.append(transaction_86)
                # print(transaction_61)
                # print(transaction_86)
                # print(mapped_ids)

                response.append(mapped_ids)
        # print(len(statement_lines), len(utr_list), len(refund_list))
        print(response)
        return response

    def get_utr(self, transaction_86):
        transaction_86 = transaction_86.split(' ')
        transaction_86 = transaction_86[0].split('/') + transaction_86[1:]

        utr = None
        imps = False
        neft = False
        neft_return = False
        for i in range(len(transaction_86)):
            var = transaction_86[i]
            if var == "IMPS":
                imps = True
            if var == "NEFT":
                neft = True
            if neft:
                if var == "RETURN" and transaction_86[i - 1] == "NEFT":
                    neft_return = True

        if imps:
            for var in transaction_86:
                if var.isnumeric() and len(var) == 12:
                    utr = var
        elif neft:
            if neft_return:
                for var in transaction_86:
                    if var.isnumeric() and len(var) == 16:
                        utr = var
            else:
                for var in transaction_86:
                    if var[:5] == "CITIN" and len(var) == 16:
                        utr = var

        return utr

    def get_refund_id(self, transaction_61):
        refund_id = None
        for start in range(len(transaction_61)):
            if transaction_61[start] == "N" and transaction_61[start - 1].isnumeric():
                for end in range(start, len(transaction_61)):
                    if transaction_61[end] == '/':
                        refund_id = transaction_61[start + 4:end]
                        break
            if refund_id:
                break
        return refund_id


cod = CodReturnRefundUTR(test_val=9)














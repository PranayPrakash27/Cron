class CitiRetrieveUTR:
    def __init__(self, statement_file):
        self.statement = statement_file.read()
        self.start()

    def start(self):
        citi_statement = self.statement
        mapped_ids = self.utr_extract(citi_statement)
        print(mapped_ids)

    def utr_extract(self, statement_lines):
        statement = statement_lines.split('\n')
        response = []

        # utr_list = []
        # statement_lines = []
        # refund_list = []

        # going through all the transactions
        for start in range(len(statement)):
            if statement[start][:4] == ":61:":
                mapped_ids = [None, None]  # [bankReference_id, UTR_number]
                end_61 = start + 1
                while end_61 < len(statement) and (
                        statement[end_61][:4] != ":86:" and statement[end_61][:5] != ":62F:" and
                        statement[end_61][:4] != ":64:" and
                        statement[end_61][:4] != ":61:"):
                    end_61 += 1

                transaction_61 = ''
                for i in range(start, end_61):
                    transaction_61 += statement[i]  # we have the :61: field of the statement

                refund_id = self.get_bankReferece_id(transaction_61)
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

    def get_bankReferece_id(self, transaction_61):
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


statement_file = open("Sample_Statement_API_4", "r")
citi_statement_retrieve = CitiRetrieveUTR(statement_file)

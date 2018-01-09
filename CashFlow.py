class CashFlow:
    def __init__(self):
        self.account_id = None
        self.account_name = None
        self.custodian_account_code = None
        self.cash_flow_type = None
        self.purpose_code = None
        self.currency_code = None
        self.cash_flow_amount = 0.0
        self.posted_date = None
        self.settlement_date = None
        self.cash_flow_status = None
        self.cash_flow_comments = None
        self.cash_flow_recurrence_id = 0
        self.trade_date = None

    def __str__(self):
        str_list = []
        str_list.append('{')
        for k, v in self.__dict__.items():
            str_list.append('%s:%s\t' % (k, v))
        str_list.append('}')
        return ''.join(str_list)

    def jsonDefault(object):
        return object.__dict__

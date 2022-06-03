#!/usr/bin/env python
"""
This module is to perform various bank transactions like 
creating new checking/savings account,deposits and withdrawals
to and from checking/savings account, obtaining new loan, 
paying EMI for loan,obtaining new credit card, 
paying off credit card balance along with relevant fees
and penalties.
"""
import setup.bank_data_store as bs
import logging
import sqlalchemy as sql
import datetime

class BankTransaction:
    def __init__(self,account_id,account_type='Inquiry',employee_id='SystemAdmin',customer_id='0',credit_score=0,mass_insert=False):
        try:
            self.account_id = account_id
            self.account_type = account_type
            self.customer_id = customer_id
            self.credit_score = credit_score
            self.employee_id = employee_id     
            global bank_setup 
            bank_setup = bs.BankSetup()       
            self.db_engine = bank_setup.get_connection()
            with self.db_engine.connect() as conn:
                table_exists = conn.execute(sql.text("SELECT COUNT(*) FROM information_schema.tables WHERE table_name ='Customers'"))
                if table_exists.scalar() == 0:
                    bank_setup.database_setup(self.db_engine)
                    if mass_insert:
                        bank_setup.insert_data('Employees',self.db_engine)
                        bank_setup.insert_data('Customers',self.db_engine)
                        bank_setup.insert_data('AccountServices',self.db_engine)
                        bank_setup.insert_data('LoanServices',self.db_engine)
                        bank_setup.insert_data('CreditCardServices',self.db_engine)
                        bank_setup.insert_data('CustomerAccountSummary',self.db_engine)
                        bank_setup.insert_data('CustomerAccountDetail',self.db_engine)
                    result_set = conn.execute(sql.text("SELECT 'Bank Application Data Setup completed'"))
                    logging.info(*result_set.fetchmany(1)[0])
                else:
                    account_exist_stmt = sql.text("SELECT COUNT(*) FROM CustomerAccountSummary WHERE AccountId = :AccountId")
                    account_exists = conn.execute(account_exist_stmt,{"AccountId":self.account_id}).scalar()
                    if account_exists == 0:
                        self.new_account_indicator = True
                        logging.info('Customer Account Does not Exist')
                    else:
                        self.new_account_indicator = False
                        logging.info('Customer Account Exists')
                   # self.new_account_detail_id = int(conn.execute(sql.text("SELECT COALESCE(MAX(AccountDetailId)+1,1) FROM CustomerAccountDetail")).scalar())
        except Exception as e:
            logging.error(e,exc_info=True)   

    @staticmethod
    def perform_transaction(account_exists_indicator,db_engine,account_id,account_type,customer_id,
                      employee_id,deposit_amount,withdraw_amount,credit_score=0,service_id=None,transaction_notes = None,account_detail_id=None):
        try:
            if withdraw_amount > 0.00:
                original_balance = 0.00 - withdraw_amount
            else:
                original_balance = deposit_amount
            with db_engine.connect() as conn:
                if account_exists_indicator == 1:
                    summary_stmt = sql.text("INSERT INTO CustomerAccountSummary (AccountId, ServiceId, CustomerId, AccountType,CreditScore,OriginalBalance,CreatedBy,UpdatedBy) "
                                            "VALUES(:AccountId, :ServiceId,:CustomerId, :AccountType,:CreditScore,:OriginalBalance,:CreatedBy,:UpdatedBy)")
                    summary_result = conn.execute(summary_stmt,{"AccountId":account_id,
                                                            "ServiceId":service_id,
                                                            "CustomerId":customer_id,
                                                            "AccountType":account_type,
                                                            "CreditScore":credit_score,
                                                            "OriginalBalance":original_balance,
                                                            "CreatedBy":employee_id,
                                                            "UpdatedBy":employee_id}).rowcount
                else:
                    summary_result = 0
                    if credit_score != 0:
                        update_summary_stmt = sql.text("UPDATE CustomerAccountSummary SET CreditScore = :CreditScore,UpdatedBy = :UpdatedBy WHERE AccountId = :AccountId")
                        summary_result = conn.execute(update_summary_stmt,{"CreditScore":credit_score,
                                                                           "UpdatedBy":employee_id,
                                                                           "AccountId":account_id}).rowcount
                detail_statement = sql.text("INSERT INTO CustomerAccountDetail (AccountDetailId,AccountId,WithdrawalAmount,DepositAmount,CreatedBy,UpdatedBy,TransactionNotes) "
                 "VALUES(:AccountDetailId,:AccountId,:WithdrawalAmount,:DepositAmount,:CreatedBy,:UpdatedBy,:TransactionNotes)")
                detail_result = conn.execute(detail_statement,{"AccountDetailId":account_detail_id,
                                                               "AccountId":account_id,
                                                               "WithdrawalAmount":withdraw_amount,
                                                               "DepositAmount":deposit_amount,
                                                               "CreatedBy":employee_id,
                                                               "UpdatedBy":employee_id,
                                                               "TransactionNotes":transaction_notes}).rowcount
                    
                return summary_result + detail_result
        except Exception as e:
            logging.error(e,exc_info=True)

    def deposit_amount(self,amount,service_id=None,transaction_notes = 'Deposit'):
            try:
                count_of_rows = 0
                count_of_rows = BankTransaction.perform_transaction(self.new_account_indicator,self.db_engine,self.account_id,
                    self.account_type,self.customer_id,self.employee_id,amount,0.00,self.credit_score,service_id,transaction_notes)
                if count_of_rows:
                    if count_of_rows > 0:
                        logging.info(f"Deposit Successful. Inserted Rowcount:{count_of_rows}")
                        print(f"${amount} Deposited to account {self.account_id}")
                    else:
                        print("Deposit UnSuccessful. Check Logs for More Information")     
                    
            except Exception as e:
                logging.error(e,exc_info=True)

    def withdraw_amount(self,amount,service_id=None,transaction_notes = 'Withdrawal'):
            try:
                count_of_rows = 0
                count_of_rows = BankTransaction.perform_transaction(self.new_account_indicator,self.db_engine,self.account_id,
                    self.account_type,self.customer_id,self.employee_id,0.00,amount,self.credit_score,service_id,transaction_notes)
                if count_of_rows:
                    if count_of_rows > 0:
                        logging.info(f"Withdraw Successful. Inserted Rowcount:{count_of_rows}")
                        print(f"${amount} Withdrawn from account {self.account_id}")
                    else:
                        print("Withdraw UnSuccessful. Check Logs for More Information")     
                    
            except Exception as e:
                logging.error(e,exc_info=True)

    def get_current_balance(self):
        try:
            with self.db_engine.connect() as conn:
                print(f"Customer Account Indicator:{self.new_account_indicator}")
                if not self.new_account_indicator:
                    balance_stmt = sql.text("SELECT cas.OriginalBalance AS original_balance, cas.CreatedTime AS transaction_created_date_time,cad.DepositAmount - cad.WithdrawAmount AS current_balance, "
                                               "concat(FirstName,' ',c.LastName) AS customer_name, "
                                               "(SELECT 1 FROM CustomerAccountDetail WHERE AccountId = cas.AccountId AND TransactionNotes LIKE '%Interest%') AS interest_indicator "
                                               "FROM CustomerAccountSummary cas "
                                         "INNER JOIN (SELECT AccountId,COALESCE(SUM(WithdrawalAmount),0.00) AS WithdrawAmount,"
                                                      "COALESCE(SUM(DepositAmount),0.00) AS DepositAmount "
                                                       "FROM CustomerAccountDetail "
                                                    "GROUP BY AccountId) cad ON cas.AccountId = cad.AccountId "
                                                                            "AND cas.AccountId =:AccountId " 
                                         "INNER JOIN Customers c ON cas.CustomerId = c.CustomerId")
                    self.balance_result = conn.execute(balance_stmt,{"AccountId":self.account_id}).fetchall() 
                    customer_name = self.balance_result[0]['customer_name']
                    self.original_balance = self.balance_result[0]['original_balance']
                    self.current_balance = self.balance_result[0]['current_balance']
                    print(f"{customer_name} with Account Id {self.account_id} has current balance ${self.current_balance}.Original Balance was ${self.original_balance}")   
                    logging.info(f"Obtained Balances:{self.balance_result[0]}") 
                else:
                    self.balance_result = conn.execute(sql.text("SELECT 0 AS original_balance, 0 current_balance, 0 AS interest_indicator")).fetchall()
                    self.original_balance = self.balance_result[0]['original_balance']
                    self.current_balance = self.balance_result[0]['current_balance']
                    print(f"Account Does Not Exist, Setting balances to 0.00:{self.balance_result[0]}")
                    logging.info(f"Account Does Not Exist, Setting balances to 0.00:{self.balance_result[0]}") 
        except Exception as e:
            logging.error(e,exc_info=True)

class AccountTransaction(BankTransaction):

    def __init__(self,account_id,service_id,employee_id='SystemAdmin',customer_id='0',credit_score=0,account_type='Checking'):
        BankTransaction.__init__(self,account_id,account_type,employee_id,customer_id,credit_score)    
        self.service_id = service_id
        try:
            if account_type in ('Checking','Savings'):
                with self.db_engine.connect() as conn:
                    terms_stmt = sql.text("SELECT * FROM AccountServices WHERE AccountServiceId = :AccountServiceId")
                    self.service_terms = conn.execute(terms_stmt,{"AccountServiceId":service_id}).fetchall()
                    if self.service_terms[0]['ServiceType'] != self.account_type:
                        print(f"Service Id Type {self.service_terms[0]['ServiceType']} does not match Account Type {self.account_type}")
                        logging.info(f"Service Id Type {self.service_terms[0]['ServiceType']} does not match Account Type {self.account_type}")
                    logging.info(f"Checking or Savings Account Transaction Initiated for Account:{self.account_id}")
            BankTransaction.get_current_balance(self)
        except Exception as e:
            logging.error(e,exc_info=True)

    @staticmethod    
    def get_transaction_detail(db_engine,account_id):
        try:
            global transaction_dict
            transaction_dict = {}
            with db_engine.connect() as conn:
                transaction_stmt = sql.text("SELECT count(*) "
                                              "FROM CustomerAccountDetail "
                                             "WHERE AccountId = :AccountId "
                                               "AND month(TransactionTime) = month(SYSDATE())")
                transaction_dict['number_of_transactions'] = conn.execute(transaction_stmt,{"AccountId":account_id}).scalar()
                withdraw_limit_stmt = sql.text("SELECT COALESCE(sum(WithdrawalAmount),0.00) "
                                                 "FROM CustomerAccountDetail "
                                                "WHERE AccountId = :AccountId "
                                                  "AND DATE(TransactionTime) = CURRENT_DATE()")
                transaction_dict['withdrawal_amount'] = conn.execute(withdraw_limit_stmt,{"AccountId":account_id}).scalar()
                monthly_fee_stmt = sql.text("SELECT 1 "
                                                 "FROM CustomerAccountDetail "
                                                "WHERE AccountId = :AccountId "
                                                  "AND month(TransactionTime) = month(SYSDATE()) "
                                                  "AND TransactionNotes Like '%Monthly%' ")
                transaction_dict['monthly_fee'] = conn.execute(monthly_fee_stmt,{"AccountId":account_id}).scalar()
        except Exception as e:
            logging.error(e,exc_info=True)

    def calculate_fees(self,service_id,number_of_transactions=0):
        try:
            self.fee_dict = {}
            if datetime.datetime.now().strftime("%d") == '01' and not transaction_dict['monthly_fee']:
                self.fee_dict['Monthly Fees'] = self.service_terms[0]['MonthlyFees']
            if number_of_transactions >= self.service_terms[0]['FreeTransactionCountPerMonth']:
                self.fee_dict['Transaction Fees'] = self.service_terms[0]['TransactionFees']
            print(f"List of Fees include:{self.fee_dict}")
            logging.info(f"List of Fees include:{self.fee_dict}")
        except Exception as e:
            logging.error(e,exc_info=True)

    def calculate_interest(self):
        try:
            if not self.balance_result[0]['interest_indicator']:
                self.interest = self.service_terms[0]['InterestRate']*self.balance_result[0]['current_balance']/100
                print(f"Interest calculated is:{self.interest}")
        except Exception as e:
            logging.error(e,exc_info=True)

    def deposit_amount(self, amount):
        try:    
            self.interest=0.00
            transaction_notes = None
            if datetime.datetime.now().strftime("%m") in ['01','04','07','10']:
                AccountTransaction.calculate_interest(self)
                if self.interest > 0.00:
                    transaction_notes = "Amount deposited With Interest for quarter"
            BankTransaction.deposit_amount(self,amount+self.interest, self.service_id,transaction_notes)
            AccountTransaction.withdraw_amount(self,0.00,True)
        except Exception as e:
            logging.error(e,exc_info=True)

    def withdraw_amount(self, amount):
        try:
            total_fees=0
            AccountTransaction.get_transaction_detail(self.db_engine,self.account_id)
            AccountTransaction.calculate_fees(self,self.service_id,transaction_dict['number_of_transactions']) 
            total_fees = sum([x for x in self.fee_dict.values()])
            print(f"Current Balance:{self.balance_result[0]['current_balance']} Withdraw Amount and Fees: {amount + total_fees}")
            if self.balance_result[0]['current_balance'] > 0 and self.balance_result[0]['current_balance'] >= amount + total_fees:
                if transaction_dict['withdrawal_amount'] + amount <= self.service_terms[0]['WithdrawalLimitPerDay']:
                    BankTransaction.withdraw_amount(self,amount, self.service_id)
                else:
                    logging.info(f"Withdraw UnSuccessful. Amount to be withdrawn greater than withdrawal limit for the day")
                    print(f"${amount} greater than withdrawal limit for the day")
            else:
                if amount > 0:
                    print(f"Insufficient funds. Current Balance available:{self.balance_result[0]['current_balance']}")
            if self.balance_result[0]['current_balance'] != 0:
                {BankTransaction.withdraw_amount(self,j, self.service_id,i) for i,j in self.fee_dict.items()}
        except Exception as e:
            logging.error(e,exc_info=True)

class LoanTransaction(AccountTransaction):

    def __init__(self,account_id,service_id,employee_id='SystemAdmin',customer_id='0',credit_score=0,amount=0.00,account_type='Car Loan'):
        AccountTransaction.__init__(self,account_id,service_id,employee_id,customer_id,credit_score,account_type)    
        try:
            with self.db_engine.connect() as conn:
                terms_stmt = sql.text("SELECT * FROM LoanServices WHERE LoanServiceId = :LoanServiceId")
                self.service_terms = conn.execute(terms_stmt,{"LoanServiceId":self.service_id}).fetchall()
                if self.service_terms[0]['ServiceType'] != self.account_type:
                    print(f"Service Id Type {self.service_terms[0]['ServiceType']} does not match Account Type {self.account_type}")
                    logging.info(f"Service Id Type {self.service_terms[0]['ServiceType']} does not match Account Type {self.account_type}")
                else:
                    if self.new_account_indicator and amount > 0.00:
                        self.original_balance = amount + (amount * (self.service_terms[0]['LoanCostPercentage']/100))

                        if self.service_terms[0]['MinimumLoanAmount'] > self.original_balance:
                            print(f"Loan Amount {amount} is less than minimum loan amount {self.service_terms[0]['MinimumLoanAmount']} possible for this service offering")
                            logging.info(f"Loan Amount {amount} is less than minimum loan amount {self.service_terms[0]['MinimumLoanAmount']} possible for this service offering")   
                        else:
                            if self.service_terms[0]['MaximumLoanAmount'] < self.original_balance:
                                print(f"Loan Amount {amount} is greater than maximum loan amount {self.service_terms[0]['MaximumLoanAmount']} possible for this service offering")
                                logging.info(f"Loan Amount {amount} is greater than maximum loan amount {self.service_terms[0]['MaximumLoanAmount']} possible for this service offering")
                            else:
                                BankTransaction.withdraw_amount(self,self.original_balance, self.service_id)
                    logging.info(f"Loan Transaction Initiated for Account:{self.account_id}")
        except Exception as e:
            logging.error(e,exc_info=True)
    
    @staticmethod    
    def get_transaction_detail(db_engine,account_id):
        try:
            global transaction_dict
            transaction_dict = {}
            with db_engine.connect() as conn:
                monthly_payment_stmt = sql.text("SELECT sum(DepositAmount) "
                                                 "FROM CustomerAccountDetail "
                                                "WHERE AccountId = :AccountId "
                                                  "AND month(TransactionTime) = month(SYSDATE())-1 "
                                                  "AND TransactionNotes Like '%Monthly%' "
                                                  "GROUP BY month(TransactionTime)")
                transaction_dict['monthly_payment'] = conn.execute(monthly_payment_stmt,{"AccountId":account_id}).scalar()
                principal_stmt = sql.text("SELECT sum(DepositAmount) "
                                                    "FROM CustomerAccountDetail "
                                                   "WHERE AccountId = :AccountId "
                                                     "AND TransactionNotes Like '%Principal%'")
                transaction_dict['principal_paid'] = conn.execute(principal_stmt,{"AccountId":account_id}).scalar()
                if not transaction_dict['principal_paid']:
                    transaction_dict['principal_paid'] = 0
        except Exception as e:
            logging.error(e,exc_info=True)

    def calculate_monthly_payment(self):
        try:
            self.monthly_payment = {}
            LoanTransaction.get_transaction_detail(self.db_engine,self.account_id)
            principal_remaining = transaction_dict['principal_paid'] - (self.balance_result[0]['original_balance'])
            intermediate_result = (1 + (self.service_terms[0]['InterestRate']/1200)) ** self.service_terms[0]['PeriodInMonths']
            monthly_installment = -(self.balance_result[0]['original_balance']) * (((self.service_terms[0]['InterestRate']/1200) * 
                                                            intermediate_result)/(intermediate_result - 1))
            print(f"Equated Monthly Installment Amount:{monthly_installment}")
            logging.info(f"Equated Monthly Installment Amount:{monthly_installment}")

            self.monthly_payment['Monthly Interest Amount'] = (self.service_terms[0]['InterestRate']/1200)*principal_remaining
            self.monthly_payment['Monthly Principal Amount'] = monthly_installment - self.monthly_payment['Monthly Interest Amount']

            if not transaction_dict['monthly_payment'] or (transaction_dict['monthly_payment'] and transaction_dict['monthly_payment'] < monthly_installment):
                if int(datetime.datetime.now().strftime("%Y")) == self.balance_result[0]['transaction_created_date_time'].year and int(datetime.datetime.now().strftime("%m")) - int(self.balance_result[0]['transaction_created_date_time'].month) < 2:
                    self.monthly_payment['Late Fees'] = 0 
                else:
                    self.monthly_payment['Late Fees'] = self.service_terms[0]['LateFee']

        except Exception as e:
            logging.error(e,exc_info=True)

    def deposit_amount(self, amount):
        try:  
            LoanTransaction.calculate_monthly_payment(self)
            {BankTransaction.deposit_amount(self,j, self.service_id,i) for i,j in self.monthly_payment.items() if 'Fees' not in i } 
            if self.monthly_payment['Late Fees'] > 0:
                BankTransaction.withdraw_amount(self,self.monthly_payment['Late Fees'],self.service_id,'Late Fees')
        except Exception as e:
            logging.error(e,exc_info=True)

    
        

                
#d = datetime.datetime.strftime(datetime.datetime.strptime('2022-06-02 14:59:56','%Y-%m-%d %H:%M:%S'),'%m')
#print(int(d))           

#a = BankTransaction('Test',mass_insert=True)



        
c = LoanTransaction('LS0000001','LS0004','E000000002','C000000003',750,250000,'Home Loan')
#print(int(datetime.datetime.now().strftime("%m")))
#print(c.balance_result[0]['transaction_created_date_time'].year)
c.deposit_amount(5000)
#c.calculate_interest()
#c.calculate_fees('AS0002',8)
#c.get_current_balance()

    

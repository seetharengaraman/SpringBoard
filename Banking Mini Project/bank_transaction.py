#!/usr/bin/env python
"""
This module is to perform various bank transactions like 
creating new checking/savings account,deposits and withdrawals
to and from checking/savings account, obtaining new loan and 
paying EMI for loan along with relevant fees
and penalties.
"""
import setup.bank_data_store as bs
import logging
import sqlalchemy as sql
import datetime
import pandas as pd
from tabulate import tabulate

class BankTransaction:
    def __init__(self,account_id,account_type='Inquiry',employee_id='SystemAdmin',customer_id='0',credit_score=0,mass_insert=False):
        try:
            self.account_id = account_id
            self.account_type = account_type
            self.customer_id = customer_id
            self.credit_score = credit_score
            self.employee_id = employee_id     
            global bank_setup,bank_store 
            bank_setup = bs.BankSetup()   
            table_exists,db_engine = bank_setup.verify_database_setup()
            bank_store = bs.BankTransactionStore(db_engine)    
            if table_exists < 6:
                bank_setup.database_setup()
                if mass_insert:
                    bank_setup.mass_insert('Employees')
                    bank_setup.mass_insert('Customers')
                    bank_setup.mass_insert('AccountServices')
                    bank_setup.mass_insert('LoanServices')
                    bank_setup.mass_insert('CustomerAccountSummary')
                    bank_setup.mass_insert('CustomerAccountDetail')
                logging.info("Banking Database Setup completed")
            else:
                account_exists = bank_store.check_if_account_exists(self.account_id)
                if account_exists == 0:
                    self.new_account_indicator = True
                    logging.info('Customer Account Does not Exist')
                else:
                    self.new_account_indicator = False
                    logging.info('Customer Account Exists')
                   # self.new_account_detail_id = int(conn.execute(sql.text("SELECT COALESCE(MAX(AccountDetailId)+1,1) FROM CustomerAccountDetail")).scalar())
        except Exception as e:
            logging.error(e,exc_info=True)   

    def deposit_amount(self,amount,service_id=None,transaction_notes = 'Deposit'):
            try:
                count_of_rows = 0
                count_of_rows = bank_store.save_transaction(self.new_account_indicator,self.account_id,
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
                count_of_rows = bank_store.save_transaction(self.new_account_indicator,self.account_id,
                    self.account_type,self.customer_id,self.employee_id,0.00,amount,self.credit_score,service_id,transaction_notes)
                if count_of_rows:
                    if count_of_rows > 0:
                        logging.info(f"Withdraw Successful. Inserted Rowcount:{count_of_rows}")
                        print(f"${amount} Withdrawn from account {self.account_id}")
                    else:
                        print("Withdraw UnSuccessful. Check Logs for More Information")     
                    
            except Exception as e:
                logging.error(e,exc_info=True)

    def get_current_balance(self,print_indicator=False):
        if not self.new_account_indicator:
            balance_result = bank_store.get_account_summary(self.account_id)
            self.customer_name = balance_result[0]['customer_name']
            self.original_balance = balance_result[0]['original_balance']
            self.current_balance = balance_result[0]['current_balance']
            self.transaction_time = balance_result[0]['transaction_time']
            self.interest_indicator = balance_result[0]['interest_indicator']
            logging.info(f"Obtained Balances:{balance_result[0]}") 
        else:
            self.original_balance = 0.00
            self.current_balance = 0.00
            self.interest_indicator = 0
            logging.info(f"Account {self.account_id} Does Not Exist") 
        if print_indicator:
            if self.current_balance > 0:
                print(f"Current Balance for {self.account_id} is {self.current_balance}")
            else:
                print(f"Current Balance for {self.account_id} is {-self.current_balance}")
    
    def get_latest_transactions(self,number_of_transactions=5):
        summary,detail = bank_store.get_transaction_detail(self.account_id,number_of_transactions)
        summary_df = pd.DataFrame(summary, columns=summary[0].keys())
        detail_df = pd.DataFrame(detail, columns=detail[0].keys())
        print(tabulate(summary_df, headers='keys', tablefmt='psql', showindex=False))
        print(tabulate(detail_df, headers='keys', tablefmt='psql', showindex=False))
        


class AccountTransaction(BankTransaction):

    def __init__(self,account_id,service_id,employee_id='SystemAdmin',customer_id='0',credit_score=0,account_type='Checking'):
        global transaction_dict
        transaction_dict = {}
        BankTransaction.__init__(self,account_id,account_type,employee_id,customer_id,credit_score)   
        self.service_id = service_id
        try:
            if account_type in ('Checking','Savings'):
                self.service_terms = bank_store.get_service_terms(self.service_id,'Account')
                if self.service_terms[0]['ServiceType'] != self.account_type:
                    print(f"Service Id Type {self.service_terms[0]['ServiceType']} does not match Account Type {self.account_type}")
                    logging.info(f"Service Id Type {self.service_terms[0]['ServiceType']} does not match Account Type {self.account_type}")
                logging.info(f"Checking or Savings Account Transaction Initiated for Account:{self.account_id}")
            self.get_current_balance()
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
            if not self.interest_indicator:
                self.interest = self.service_terms[0]['InterestRate']*self.current_balance/100
                print(f"Interest calculated is:{self.interest}")
        except Exception as e:
            logging.error(e,exc_info=True)

    def deposit(self, amount):
        try:    
            self.interest=0.00
            transaction_notes = None
            if datetime.datetime.now().strftime("%m") in ['01','04','07','10']:
                self.calculate_interest()
                if self.interest > 0.00:
                    transaction_notes = "Amount deposited With Interest for quarter"
                else:
                    self.interest = 0.00
            self.deposit_amount(amount+self.interest, self.service_id,transaction_notes)
            self.withdraw_amount(0.00)
        except Exception as e:
            logging.error(e,exc_info=True)

    def withdraw(self, amount):
        try:
            total_fees=0
            transaction_dict = bank_store.get_account_detail(self.account_id,'Account')
            self.calculate_fees(self.service_id,transaction_dict['number_of_transactions']) 
            total_fees = sum([x for x in self.fee_dict.values()])
            total_withdraw_amount = amount + total_fees
            print(f"Current Balance:{self.current_balance} Withdraw Amount and Fees: {total_withdraw_amount}")
            if self.current_balance > 0 and self.current_balance >= total_withdraw_amount:
                if transaction_dict['withdrawal_amount'] + amount <= self.service_terms[0]['WithdrawalLimitPerDay']:
                    self.withdraw_amount(amount, self.service_id)
                else:
                    logging.info(f"Withdraw UnSuccessful. Amount to be withdrawn greater than withdrawal limit for the day")
                    print(f"${amount} greater than withdrawal limit for the day")
            else:
                if amount > 0:
                    print(f"Insufficient funds. Current Balance available:{self.current_balance}")
            if self.current_balance != 0:
                {self.withdraw_amount(j, self.service_id,i) for i,j in self.fee_dict.items()}
        except Exception as e:
            logging.error(e,exc_info=True)

class LoanTransaction(AccountTransaction):

    def __init__(self,account_id,service_id,employee_id='SystemAdmin',customer_id='0',credit_score=0,amount=0.00,account_type='Car Loan'):
        AccountTransaction.__init__(self,account_id,service_id,employee_id,customer_id,credit_score,account_type)    
        try:
            self.service_terms = bank_store.get_service_terms(self.service_id,'Loan')
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
                            self.withdraw_amount(self.original_balance, self.service_id)
                            self.new_account_indicator = False
                    logging.info(f"Loan Transaction Initiated for Account:{self.account_id}")
        except Exception as e:
            logging.error(e,exc_info=True)

    def calculate_monthly_payment(self):
        try:
            self.monthly_payment = {}
            transaction_dict = {}
            transaction_dict = bank_store.get_account_detail(self.account_id,'Loan')
            principal_remaining = transaction_dict['principal_paid'] - (self.original_balance)
            intermediate_result = (1 + (self.service_terms[0]['InterestRate']/1200)) ** self.service_terms[0]['PeriodInMonths']
            monthly_installment = -(self.original_balance) * (((self.service_terms[0]['InterestRate']/1200) * 
                                                            intermediate_result)/(intermediate_result - 1))
            print(f"Equated Monthly Installment Amount:{monthly_installment}")
            logging.info(f"Equated Monthly Installment Amount:{monthly_installment}")
            self.monthly_payment['Monthly Interest Amount'] = (self.service_terms[0]['InterestRate']/1200)*principal_remaining
            self.monthly_payment['Monthly Principal Amount'] = monthly_installment - self.monthly_payment['Monthly Interest Amount']
            print(f"Monthly Interest Amount:{self.monthly_payment['Monthly Interest Amount']}")
            print(f"Monthly Principal Amount:{self.monthly_payment['Monthly Principal Amount']}")
            if not transaction_dict['monthly_payment'] or (transaction_dict['monthly_payment'] and transaction_dict['monthly_payment'] < monthly_installment):
                if int(datetime.datetime.now().strftime("%Y")) == self.transaction_time.year and int(datetime.datetime.now().strftime("%m")) - int(self.transaction_time.month) < 2:
                    self.monthly_payment['Late Fees'] = 0 
                else:
                    self.monthly_payment['Late Fees'] = self.service_terms[0]['LateFee']

        except Exception as e:
            logging.error(e,exc_info=True)

    def deposit(self, amount):
        try:  
            self.calculate_monthly_payment()
            if amount > self.monthly_payment['Monthly Interest Amount'] + self.monthly_payment['Monthly Principal Amount']:
                self.monthly_payment['Additional Principal Amount'] = amount - (self.monthly_payment['Monthly Interest Amount'] + self.monthly_payment['Monthly Principal Amount'])
            {self.deposit_amount(j, self.service_id,i) for i,j in self.monthly_payment.items() if 'Fees' not in i } 
            if self.monthly_payment['Late Fees'] > 0:
                self.withdraw_amount(self.monthly_payment['Late Fees'],self.service_id,'Late Fees')
        except Exception as e:
            logging.error(e,exc_info=True)

    
        

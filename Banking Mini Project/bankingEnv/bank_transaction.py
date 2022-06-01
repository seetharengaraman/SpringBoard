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

class BankTransaction:
    def __init__(self,account_id,account_type,customer_id,credit_score,employee_id):
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
                    bank_setup.insert_data('Employees',self.db_engine)
                    bank_setup.insert_data('Customers',self.db_engine)
                    bank_setup.insert_data('AccountServices',self.db_engine)
                    bank_setup.insert_data('LoanServices',self.db_engine)
                    bank_setup.insert_data('CreditCardServices',self.db_engine)
                    result_set = conn.execute(sql.text("SELECT 'Bank Application Data Setup completed'"))
                    logging.info(*result_set.fetchmany(1)[0])
                else:
                    account_exist_stmt = sql.text("SELECT COUNT(*) FROM CustomerAccountSummary WHERE AccountId = :AccountId")
                    account_exists = conn.execute(account_exist_stmt,{"AccountId":self.account_id}).scalar()
                    if account_exists == 0:
                        self.new_account_indicator = 1
                        logging.info('New Account should be Created')
                    else:
                        self.new_account_indicator = 0
                        logging.info('Customer Account Exists')
                    self.new_account_detail_id = int(conn.execute(sql.text("SELECT COALESCE(MAX(AccountDetailId)+1,1) FROM CustomerAccountDetail")).scalar())
        except Exception as e:
            logging.error(e,exc_info=True)

    @staticmethod
    def perform_transaction(account_exists_indicator,db_engine,account_id,account_detail_id,account_type,customer_id,
                      employee_id,deposit_amount,withdraw_amount,credit_score=0):
        try:
            if withdraw_amount > 0.00:
                original_balance = 0.00 - withdraw_amount
            else:
                original_balance = deposit_amount   
            with db_engine.connect() as conn:
                if account_exists_indicator == 1:
                    summary_stmt = sql.text("INSERT INTO CustomerAccountSummary (AccountId, CustomerId, AccountType,CreditScore,OriginalBalance,CreatedBy,UpdatedBy) VALUES(:AccountId, :CustomerId, :AccountType,:CreditScore,:OriginalBalance,:CreatedBy,:UpdatedBy)")
                    summary_result = conn.execute(summary_stmt,{"AccountId":account_id,
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
                detail_statement = sql.text("INSERT INTO CustomerAccountDetail (AccountDetailId,AccountId,WithdrawalAmount,DepositAmount,CreatedBy,UpdatedBy) VALUES(:AccountDetailId,:AccountId,:WithdrawalAmount,:DepositAmount,:CreatedBy,:UpdatedBy)")
                detail_result = conn.execute(detail_statement,{"AccountDetailId":account_detail_id,
                                                               "AccountId":account_id,
                                                               "WithdrawalAmount":withdraw_amount,
                                                               "DepositAmount":deposit_amount,
                                                               "CreatedBy":employee_id,
                                                               "UpdatedBy":employee_id}).rowcount
                    
                return summary_result + detail_result
        except Exception as e:
            logging.error(e,exc_info=True)

    def deposit_amount(self,amount):
            try:
                count_of_rows = 0
                count_of_rows = BankTransaction.perform_transaction(self.new_account_indicator,self.db_engine,self.account_id,self.new_account_detail_id,
                    self.account_type,self.customer_id,self.employee_id,amount,0.00,self.credit_score)
                if count_of_rows:
                    if count_of_rows > 0:
                        logging.info(f"Deposit Successful. Inserted Rowcount:{count_of_rows}")
                        print(f"${amount} Deposited to account {self.account_id}")
                    else:
                        print("Deposit UnSuccessful. Check Logs for More Information")     
                    
            except Exception as e:
                logging.error(e,exc_info=True)
    def withdraw_amount(self,amount):
            try:
                count_of_rows = 0
                count_of_rows = BankTransaction.perform_transaction(self.new_account_indicator,self.db_engine,self.account_id,self.new_account_detail_id,
                    self.account_type,self.customer_id,self.employee_id,0.00,amount,self.credit_score)
                if count_of_rows:
                    if count_of_rows > 0:
                        logging.info(f"Withdraw Successful. Inserted Rowcount:{count_of_rows}")
                        print(f"${amount} Withdrawn from account {self.account_id}")
                    else:
                        print("Withdraw UnSuccessful. Check Logs for More Information")     
                    
            except Exception as e:
                logging.error(e,exc_info=True)

c = BankTransaction('AS00000004','Car Loan','C000000004','700','E000000001')
c.withdraw_amount(10000.00)
    

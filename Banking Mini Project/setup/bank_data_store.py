#!/usr/bin/env python
"""
 This module takes care of setting up Banking 
 Database,creating necessary tables to host employees,
 customers, Loan services, Account services, Customer 
 Account Summary and Account Detail and method to mass 
 insert data from an excel file. It also sets up basic
 logging configuration. It is also a transaction store
 that allows to query account details as well as saving
 deposit and withdrawal transactions.
"""
import sqlalchemy as sql
from sqlalchemy import *
import logging
import datetime
import pandas as pd

class TZDateTime(sql.TypeDecorator):
    impl = sql.DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            if not value.tzinfo:
                raise TypeError("TimeZone is required")
            value = value.astimezone(datetime.timezone.utc).replace(
                tzinfo=None
            )
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value

class BankSetup:
    def __init__(self,
                file_name = 'Banking_Logs.txt',
                connect_string = "mysql+pymysql://user:password@localhost/banking_system"
                ):
        self.file_format = '%(asctime)s %(module)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-2s [%(process)d] %(message)s'
        logging.basicConfig(level=logging.INFO,
                            filename=file_name,
                            format=self.file_format,
                            datefmt='%Y-%m-%d %H:%M:%S'
                           )
        self.file_name = file_name
        self.connect_string = connect_string

    def get_connection(self):
        try:
            self.db_engine = sql.create_engine(self.connect_string)
        except Exception as e:
            logging.error(e,exc_info=True)
    
    def database_setup(self):
        try:
            self.get_connection()
            with self.db_engine.connect() as conn:
                result_set = conn.execute(sql.text("select 'Connected to create tables!'"))
                logging.info(*result_set.fetchmany(1)[0])
            metadata_obj = MetaData()
            Employees = Table('Employees',metadata_obj,
                    Column('Id',String(20),primary_key=True),
                    Column('FirstName',String(150),nullable=False),
                    Column('LastName',String(150),nullable=False),
                    Column('AddressLine1',String(150),nullable=False),
                    Column('AddressLine2',String(150),nullable=True),
                    Column('City',String(100),nullable=False),
                    Column('State',String(100),nullable=True),
                    Column('PostalCode',String(50),nullable=False),
                    Column('Country',String(50),nullable=False),
                    Column('ServiceHandled',Enum('Account Maintenance','Loans','Credit Cards')),
                    Column('HandlesHomeLoan',Enum('Yes','No'), server_default="No"),
                    Column('HandlesPersonalLoan',Enum('Yes','No'), server_default="No"),
                    Column('HandlesBusinessLoan',Enum('Yes','No'), server_default="No"),
                    Column('CreatedTime',TZDateTime,server_default=func.now()),
                    Column('UpdatedTime',TZDateTime,server_default=func.now()),
                    Column('CreatedBy',String(20),nullable=False),
                    Column('UpdatedBy',String(20),nullable=False)
                        )

            Customers = Table('Customers',metadata_obj,
                    Column('CustomerId',String(20),primary_key=True),
                    Column('FirstName',String(150),nullable=False),
                    Column('LastName',String(150),nullable=False),
                    Column('AddressLine1',String(150),nullable=False), 	
                    Column('AddressLine2',String(150),nullable=True),
                    Column('City',String(100),nullable=False),
                    Column('State',String(100),nullable=True),
                    Column('PostalCode',String(50),nullable=False),
                    Column('Country',String(50),nullable=False),
                    Column('CustomerType',Enum('Individual','Business'),nullable=True),
                    Column('CompanyName',String(200),nullable=True),
                    Column('PersonalBankerId',String(20),ForeignKey('Employees.Id'),nullable=True),
                    Column('CreatedTime',TZDateTime,server_default=func.now()),
                    Column('UpdatedTime',TZDateTime,server_default=func.now()),
                    Column('CreatedBy',String(20),ForeignKey('Employees.Id'),nullable=False),
                    Column('UpdatedBy',String(20),ForeignKey('Employees.Id'),nullable=False)
                        )
            AccountServices = Table('AccountServices',metadata_obj,
                    Column('AccountServiceId',String(10),primary_key=True),
                    Column('ServiceType',Enum('Savings','Checking')),
                    Column('MaximumCreditScore',Integer,nullable=False),
                    Column('MinimumBalance',Float,server_default="500.00",nullable=False),
                    Column('MonthlyFees',Float,server_default="0.00",nullable=False),
                    Column('FreeTransactionCountPerMonth',Integer,server_default="0",nullable=False),
                    Column('TransactionFees',Float,server_default="0.00",nullable=False),
                    Column('InterestRate',Float,server_default="0.00",nullable=False),
                    Column('WithdrawalLimitPerDay',Float,server_default="0.00",nullable=False),
                    Column('IsDirectDeposit',Enum('Yes','No'), server_default="No",nullable=False),
                    Column('CreatedTime',TZDateTime,server_default=func.now()),
                    Column('UpdatedTime',TZDateTime,server_default=func.now()),
                    Column('CreatedBy',String(20),ForeignKey('Employees.Id'),nullable=False),
                    Column('UpdatedBy',String(20),ForeignKey('Employees.Id'),nullable=False)
                    )
            LoanServices = Table('LoanServices',metadata_obj,
                Column('LoanServiceId',String(10),primary_key=True),
                Column('ServiceType',Enum('Car Loan','Business Loan','Home Loan','Personal Loan')),
                Column('MinimumLoanAmount',Float,nullable=False),
                Column('MaximumLoanAmount',Float,nullable=False),
                Column('InterestRate',Float,nullable=False),
                Column('PeriodInMonths',Integer,nullable=False),
                Column('LoanCostPercentage',Float,server_default="0.01",nullable=False),
                Column('LateFee',Float,server_default="0.00",nullable=False),
                Column('CreatedTime',TZDateTime,server_default=func.now()),
                Column('UpdatedTime',TZDateTime,server_default=func.now()),
                Column('CreatedBy',String(20),ForeignKey('Employees.Id'),nullable=False),
                Column('UpdatedBy',String(20),ForeignKey('Employees.Id'),nullable=False)
                )

            CustomerAccountSummary = Table('CustomerAccountSummary',metadata_obj,
                                            Column('AccountId',String(30),primary_key=True),
                                            Column('CustomerId',String(20),ForeignKey('Customers.CustomerId'),nullable=False),
                                            Column('AccountType',Enum('Savings','Checking','Car Loan','Business Loan','Credit Card','Home Loan', 'Personal Loan')),
                                            Column('ServiceId',String(10),nullable=True),
                                            Column('CreditScore',Integer,nullable=False),
                                            Column('OriginalBalance',Float,server_default="0.00",nullable=False),
                                            Column('CreatedTime',TZDateTime,server_default=func.now()),
                                            Column('UpdatedTime',TZDateTime,server_default=func.now()),
                                            Column('CreatedBy',String(20),ForeignKey('Employees.Id'),nullable=False),
                                            Column('UpdatedBy',String(20),ForeignKey('Employees.Id'),nullable=False)
                                                )
                
            CustomerAccountDetail = Table('CustomerAccountDetail',metadata_obj,
                                            Column('AccountDetailId',Integer,primary_key=True),
                                            Column('AccountId',String(30),ForeignKey('CustomerAccountSummary.AccountId'),nullable=False),
                                            Column('TransactionTime',TZDateTime,server_default=func.now()),
                                            Column('WithdrawalAmount',Float,server_default="0.00",nullable=False),
                                            Column('DepositAmount',Float,server_default="0.00",nullable=False),
                                            Column('TransactionNotes',String(300),nullable=True),
                                            Column('CreatedTime',TZDateTime,server_default=func.now()),
                                            Column('UpdatedTime',TZDateTime,server_default=func.now()),
                                            Column('CreatedBy',String(20),ForeignKey('Employees.Id'),nullable=False),
                                            Column('UpdatedBy',String(20),ForeignKey('Employees.Id'),nullable=False)
                                                )
            metadata_obj.create_all(self.db_engine)
        except Exception as e:
            logging.error(e,exc_info=True)
        
    def mass_insert(self,
                    table_name,
                    data_file = '/Users/renga/Documents/SpringBoard Data Engineering Course/bankingEnv/data/BankingData.xlsx'):
        try:            
            df = pd.read_excel(data_file,index_col=0,sheet_name=table_name) 
           ## print(df.head())
            df.to_sql(table_name,self.db_engine,if_exists='append')
        except Exception as e:
            logging.error(e,exc_info=True)
    
    def verify_database_setup(self):
        self.get_connection()
        with self.db_engine.connect() as conn:
            table_exists = conn.execute(sql.text("SELECT COUNT(*) "
                                                    "FROM information_schema.tables " 
                                                    "WHERE table_name IN "
                                                    "('Customers','Employees','LoanServices',"
                                                    "'AccountServices','CustomerAccountSummary',"
                                                    "'CustomerAccountDetail')"))
            return table_exists.scalar(),self.db_engine
    
class BankTransactionStore:
    def __init__(self,db_engine):
        self.db_engine = db_engine

    def check_if_account_exists(self, account_id):
        with self.db_engine.connect() as conn:
            account_exist_stmt = sql.text("SELECT COUNT(*) "
                                          "FROM CustomerAccountSummary "
                                          "WHERE AccountId = :AccountId")
            return conn.execute(account_exist_stmt,{"AccountId":account_id}).scalar()


    def save_transaction(self,account_exists_indicator,account_id,account_type,customer_id,
                      employee_id,deposit_amount,withdraw_amount,credit_score=0,service_id=None,transaction_notes = None,account_detail_id=None):
        try:
            if withdraw_amount > 0.00:
                original_balance = 0.00 - withdraw_amount
            else:
                original_balance = deposit_amount
            with self.db_engine.connect() as conn:
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

    def get_account_summary(self,account_id):
        try:
            with self.db_engine.connect() as conn:
                account_stmt = sql.text("SELECT cas.OriginalBalance AS original_balance, cas.CreatedTime AS transaction_time,cad.DepositAmount - cad.WithdrawAmount AS current_balance, "
                                               "concat(FirstName,' ',c.LastName) AS customer_name, "
                                               "(SELECT 1 FROM CustomerAccountDetail WHERE AccountId = cas.AccountId AND TransactionNotes LIKE '%Interest%') AS interest_indicator "
                                               "FROM CustomerAccountSummary cas "
                                         "INNER JOIN (SELECT AccountId,COALESCE(SUM(WithdrawalAmount),0.00) AS WithdrawAmount,"
                                                      "COALESCE(SUM(DepositAmount),0.00) AS DepositAmount "
                                                       "FROM CustomerAccountDetail "
                                                    "GROUP BY AccountId) cad ON cas.AccountId = cad.AccountId "
                                                                            "AND cas.AccountId =:AccountId " 
                                         "INNER JOIN Customers c ON cas.CustomerId = c.CustomerId")
                return conn.execute(account_stmt,{"AccountId":account_id}).fetchall() 
        except Exception as e:
            logging.error(e,exc_info=True)
    
    def get_transaction_detail(self,account_id,number_of_transactions=5):
        try:
            with self.db_engine.connect() as conn:
                account_stmt = sql.text("SELECT cas.AccountId AS account_id,cas.CustomerId AS customer_id,cas.AccountType,abs(cas.OriginalBalance) AS original_balance,"
                                               "round(abs(cad.DepositAmount-cad.WithdrawAmount),2) AS current_balance "
                                               "FROM CustomerAccountSummary cas INNER JOIN (SELECT AccountId,COALESCE(SUM(WithdrawalAmount),0.00) AS WithdrawAmount,"
                                               "COALESCE(SUM(DepositAmount),0.00) AS DepositAmount "
                                                       "FROM CustomerAccountDetail "
                                                    "GROUP BY AccountId) cad ON cas.AccountId = cad.AccountId "
                                        "WHERE cas.AccountId =:AccountId")
                summary_data = conn.execute(account_stmt,{"AccountId":account_id}).fetchall() 
                detail_stmt = sql.text("SELECT TransactionTime AS transaction_time,WithdrawalAmount AS withdraw_amount,DepositAmount AS deposit_amount,TransactionNotes AS transaction_notes "
                                               "FROM CustomerAccountDetail "
                                               "WHERE AccountId =:AccountId "
                                               "ORDER BY TransactionTime DESC")
                detail_data = conn.execute(detail_stmt,{"AccountId":account_id}).fetchmany(number_of_transactions)
            return summary_data,detail_data   
        except Exception as e:
            logging.error(e,exc_info=True)
    
    def get_service_terms(self,service_id,service_type ='Account'):
        try:
            with self.db_engine.connect() as conn:
                if service_type == 'Account':
                    terms_stmt = sql.text("SELECT * FROM AccountServices WHERE AccountServiceId = :AccountServiceId")
                    return conn.execute(terms_stmt,{"AccountServiceId":service_id}).fetchall()
                if service_type == 'Loan':
                    terms_stmt = sql.text("SELECT * FROM LoanServices WHERE LoanServiceId = :LoanServiceId")
                    return conn.execute(terms_stmt,{"LoanServiceId":service_id}).fetchall()
        except Exception as e:
            logging.error(e,exc_info=True) 

    def get_account_detail(self,account_id,service_type ='Account'):
        try:
            account_detail = {}
            with self.db_engine.connect() as conn:
                if service_type =='Account':
                    transaction_stmt = sql.text("SELECT count(*) "
                                              "FROM CustomerAccountDetail "
                                             "WHERE AccountId = :AccountId "
                                               "AND month(TransactionTime) = month(SYSDATE())")
                    account_detail['number_of_transactions'] = conn.execute(transaction_stmt,{"AccountId":account_id}).scalar()
                    withdraw_limit_stmt = sql.text("SELECT COALESCE(sum(WithdrawalAmount),0.00) "
                                                 "FROM CustomerAccountDetail "
                                                "WHERE AccountId = :AccountId "
                                                  "AND DATE(TransactionTime) = CURRENT_DATE()")
                    account_detail['withdrawal_amount'] = conn.execute(withdraw_limit_stmt,{"AccountId":account_id}).scalar()
                    monthly_fee_stmt = sql.text("SELECT 1 "
                                                 "FROM CustomerAccountDetail "
                                                "WHERE AccountId = :AccountId "
                                                  "AND month(TransactionTime) = month(SYSDATE()) "
                                                  "AND TransactionNotes Like '%Monthly Fee%' ")
                    account_detail['monthly_fee'] = conn.execute(monthly_fee_stmt,{"AccountId":account_id}).scalar()
                if service_type =='Loan':
                    monthly_payment_stmt = sql.text("SELECT sum(DepositAmount) "
                                                 "FROM CustomerAccountDetail "
                                                "WHERE AccountId = :AccountId "
                                                  "AND month(TransactionTime) = month(SYSDATE())-1 "
                                                  "AND TransactionNotes Like '%Monthly%' "
                                                  "GROUP BY month(TransactionTime)")
                    account_detail['monthly_payment'] = conn.execute(monthly_payment_stmt,{"AccountId":account_id}).scalar()
                    principal_stmt = sql.text("SELECT sum(DepositAmount) "
                                                    "FROM CustomerAccountDetail "
                                                   "WHERE AccountId = :AccountId "
                                                     "AND TransactionNotes Like '%Principal%'")
                    account_detail['principal_paid'] = conn.execute(principal_stmt,{"AccountId":account_id}).scalar()
                    if not account_detail['principal_paid']:
                        account_detail['principal_paid'] = 0
                return account_detail
        except Exception as e:
            logging.error(e,exc_info=True)  


        
          


    
        












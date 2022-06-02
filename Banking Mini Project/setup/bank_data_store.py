#!/usr/bin/env python
"""
 This module takes care of setting up Banking 
 Database,creating necessary tables to host employees,
 customers, Loan, Credit Card, Account services, Customer 
 Account Summary and Account Detail and method to mass 
 insert data from an excel file. It also sets up basic
 logging configuration.
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
                connect_string = "mysql+pymysql://srengaraman:testDB123!@localhost/banking_system"
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
            return sql.create_engine(self.connect_string)
        except Exception as e:
            logging.error(e,exc_info=True)
    
    def database_setup(self,db_engine):
        try:
            with db_engine.connect() as conn:
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
                Column('Period',Integer,nullable=False),
                Column('PeriodType',Enum('Days','Months','Years'),nullable=False),
                Column('LoanCostPercentage',Float,server_default="0.01",nullable=False),
                Column('MinimumMonthsBeforePrepayment',Integer,server_default="1",nullable=False),
                Column('PrepaymentFeePercentage',Float,server_default="0.01",nullable=False),
                Column('CreatedTime',TZDateTime,server_default=func.now()),
                Column('UpdatedTime',TZDateTime,server_default=func.now()),
                Column('CreatedBy',String(20),ForeignKey('Employees.Id'),nullable=False),
                Column('UpdatedBy',String(20),ForeignKey('Employees.Id'),nullable=False)
                )

            CreditCardServices = Table('CreditCardServices',metadata_obj,
                Column('CreditCardServiceId',String(10),primary_key=True),
                Column('ServiceType',Enum('Visa','MasterCard')),
                Column('MaximumCreditLimit',Float,nullable=False),
                Column('MinimumCreditScore',Integer,server_default="0",nullable=False),
                Column('OneYearSecurityDeposit',Float,server_default="0.00",nullable=False),
                Column('InterestRate',Float,nullable=False),
                Column('YearlyFees',Float,server_default="0.00",nullable=False),
                Column('WithdrawalLimitPerDay',Float,server_default="0.00",nullable=False),
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
            metadata_obj.create_all(db_engine)
        except Exception as e:
            logging.error(e,exc_info=True)
        
    def insert_data(self,
                    table_name,
                    db_engine,
                    data_file = '/Users/renga/Documents/SpringBoard Data Engineering Course/SpringBoard/Banking Mini Project/bankingEnv/data/BankingData.xlsx'):
        try:            
            df = pd.read_excel(data_file,index_col=0,sheet_name=table_name) 
            print(df.head())
            df.to_sql(table_name,db_engine,if_exists='append')
        except Exception as e:
            logging.error(e,exc_info=True)
          


    
        











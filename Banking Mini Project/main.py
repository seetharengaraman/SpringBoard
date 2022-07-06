#!/usr/bin/env python
"""
This module is the presentation layer for the banking transaction.
This will be used by Bank employees to enter required Customer and Account
information and perform various transactions, choosing from the menu options
"""
import bank_transaction as bt
import sys
class Menu:
    def __init__(self):
        self.customer_id = input("Enter Customer Id:")
        self.account_id = input("Enter Account Number:")
        self.account_type = input("Enter Account Type:")
        self.service_id = input("Enter Account Service Term Id:")
        self.employee_id = input("Enter Employee Id used to create account:")
        self.credit_score = int(input("Enter Credit Score:"))

    def initial_menu(self):
        while True:
            user_type1 = input \
                    (
                    'Welcome To the Bank!\n To access Account Services enter A, To access Loan Services enter B or any other key to exit\n')
            if user_type1 not in ["A", "B"]:
                sys.exit()
            else:
                break
        return user_type1
    def access_account_menu(self):
        while True:
            choice = input("Enter 1 to display balance\n"
                           "Enter 2 to deposit money\n"
                           "Enter 3 to withdraw money\n"
                           "Enter 4 to fetch latest transactions\n"
                           "Enter 5 to exit transaction\n")
            if choice not in ["1", "2", "3", "4","5"]:
                print("Please enter 1, 2, 3, 4 or 5")
            else:
                break
        return choice

    def access_loan_menu(self):
        while True:
            choice = input("Enter 1 to add a new loan\n"
                           "Enter 2 to get current balance \n"
                           "Enter 3 to pay EMI online\n"
                           "Enter 4 to fetch Equated Monthly Installment Amount\n"
                           "Enter 5 to fetch Latest transactions\n"
                           "Enter 6 to exit transaction\n")
            if choice not in ["1", "2", "3", "4","5","6"]:
                print("Please enter 1, 2, 3, 4, 5 or 6")
            else:
                break
        return choice

    def previous_page(self):
        while True:
            return input("Would you like to return to the previous page? Enter yes or no:")[0].lower() == 'y'

    def run(self):

            user_type1 = self.initial_menu()
            if user_type1 == 'A':

                while True:
                    account_object = bt.AccountTransaction(self.account_id,self.service_id,self.employee_id,self.customer_id,self.credit_score,self.account_type)
                    user_choice = self.access_account_menu()
                    if user_choice == '1':
                        account_object.get_current_balance(True)
                    elif user_choice == '2':
                        self.amount = float(input("Enter Deposit Amount:"))
                        account_object.deposit(self.amount)
                    elif user_choice == '3':
                        self.amount = float(input("Enter Withdrawal Amount:"))
                        account_object.withdraw(self.amount)
                    elif user_choice == '4':
                        self.number_of_transactions = int(input("Enter Number of Transactions to View:"))
                        account_object.get_latest_transactions(self.number_of_transactions)
                    elif user_choice =='5':
                        break
                    else:
                        continue
                    if not self.previous_page():
                        break
            if user_type1 == 'B':

                while True:
                    self.amount = 0.00
                    user_choice = self.access_loan_menu()
                    if user_choice == '1':
                        self.amount = float(input("Enter Loan Amount:"))
                    loan_object = bt.LoanTransaction(self.account_id,self.service_id,self.employee_id,self.customer_id,self.credit_score,self.amount,self.account_type)
                    if user_choice == '2':
                        loan_object.get_current_balance(True)
                    elif user_choice == '3':
                        self.amount = float(input("Enter Payment Amount:"))
                        loan_object.deposit(self.amount)
                    elif user_choice == '4':
                        loan_object.calculate_monthly_payment()
                    elif user_choice == '5':
                        self.number_of_transactions = int(input("Enter Number of Transactions to View:"))
                        loan_object.get_latest_transactions(self.number_of_transactions)
                    elif user_choice =='6':
                        break
                    else:
                        continue
                    if not self.previous_page():
                        break
                            

if __name__ == '__main__':
    menu = Menu()
    Menu.run(menu)
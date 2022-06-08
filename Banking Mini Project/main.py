import bank_transaction as bt
import pandas as pd
class Menu:
    def __init__(self):
        pass

    def user_menu(self):
        while True:
            choice = input("Enter 1 to access existing account\n"
                           "Enter 2 to open new account\n"
                           "Enter 3 to request for loan service\n"
                           "Enter 4 to exit\n"
                           )
            if choice not in ["1", "2", "3", "4"]:
                print("Please enter 1, 2, 3 or 4")
            else:
                return choice

    def access_account_menu(self):
        while True:
            choice = input("Enter 1 to display balance\n"
                           "Enter 2 to withdraw money\n"
                           "Enter 3 to deposit money\n"
                           "Enter 4 to return to the main menu\n")
            if choice not in ["1", "2", "3", "4"]:
                print("Please enter 1, 2, 3 or 4")
            else:
                break
        return choice

    def access_loan_menu(self):
        while True:
            choice = input("Enter 1 to request new loan\n"
                           "Enter 2 to get current balance\n"
                           "Enter 3 to pay EMI online\n"
                           "Enter 4 to return to the main menu\n")
            if choice not in ["1", "2", "3","4"]:
                print("Please enter 1, 2, 3 or 4")
            else:
                break
        return choice

    def previous_page(self):
        while True:
            return input("Would you like to return to the previous page? Enter yes or no:")[0].lower() == 'y'

    def run(self):

        while True:
            user_type1 = input \
                    (
                    'Welcome To the Bank!\n To access Account Services enter A, To access Loan Services enter B or any other key to exit')

            if user_type1 == 'A':

                while True:
                    user_choice = self.user_menu()
                    if user_choice == '1':
                        next_choice = self.access_account_menu()
                        if next_choice == '1':
                            
                           customerobj.open_account(customerid=customerid)
                            print \
                                    (
                                    'Please restart system and login with your credentials to access menu for '
                                    'existing customers')
                            sys.exit()

                        elif new_user_choice == '2':
                            service_choice = input \
                                    (
                                    'To request a service: request credit card or loan our bank policy dictates you '
                                    'need an account open with us.\n To open an account, enter "Y" or press any key '
                                    'to exit')

                            while True:
                                if service_choice == "Y":
                                    customerobj.open_account(customerid=customerid)
                                    print \
                                            (
                                            'Please restart system and login with your credentials to access menu for '
                                            'existing customers')
                                    sys.exit()
                                else:
                                    print('goodbye..')
                                    sys.exit()

                        elif new_user_choice == "3":
                            break
                        else:
                            continue


                else:
                    break

            else:
                break


if __name__ == '__main__':
    menu = Menu()
    Menu.run(menu)
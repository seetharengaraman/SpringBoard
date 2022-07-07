Banking Miniproject consists of:

1. Database Layer (setup/bank_data_store.py)

This module takes care of setting up Banking Database,creating necessary tables to host employees,customers, Loan services, Account services, Customer Account Summary and Account Detail and method to mass insert data from an excel file. It also sets up basic logging configuration. It is also a transaction store that allows to query account details as well as saving deposit and withdrawal transactions.

2. Transaction Layer (bank_transaction.py)

This module is to perform various bank transactions like creating new checking/savings account,deposits and withdrawals to and from checking/savings account, obtaining new loan and paying EMI for loan along with relevant fees and penalties.

3. Presentation Layer (main.py)

This module is used by Bank employees to enter required Customer and Account information and perform various transactions, choosing from the menu options. This project can be initiated by running the main.py (python3 main.py)

There is an Outputs.docx file that provides results of sample scenarios covered by this program

There is also a presentation (.pptx file) that holds the Class UML diagram and ER diagram. The same is also displayed below.
![image](https://user-images.githubusercontent.com/66568505/177687645-e515b225-abde-4058-8507-063017dc5176.png)
![image](https://user-images.githubusercontent.com/66568505/177687688-5dda5517-d971-4fdd-8949-cd72196a5853.png)




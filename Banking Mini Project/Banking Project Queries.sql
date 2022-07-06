use banking_system;

SELECT * FROM CustomerAccountSummary;

SELECT * FROM CustomerAccountDetail;

SELECT * FROM Customers;

SELECT sysdate();

INSERT INTO Customers VALUES ('C000000007','Rajesh','Sinha','Leonardo Street','','Cartersville','GA','30101','US','Individual','','E000000004',sysdate(),sysdate(),'E000000001','E000000001');

SELECT * FROM LoanServices;

SELECT * FROM AccountServices;

SELECT cas.AccountId AS account_id,cas.CustomerId AS customer_id,cas.AccountType,abs(cas.OriginalBalance) AS original_balance,
round(abs(cad.DepositAmount-cad.WithdrawAmount),2) AS current_balance
FROM CustomerAccountSummary cas INNER JOIN (SELECT AccountId,COALESCE(SUM(WithdrawalAmount),0.00) AS WithdrawAmount,
                                                      COALESCE(SUM(DepositAmount),0.00) AS DepositAmount 
                                                       FROM CustomerAccountDetail 
                                                    GROUP BY AccountId) cad ON cas.AccountId = cad.AccountId 
WHERE cas.AccountId='LS00000003';

SELECT TransactionTime AS transaction_time,WithdrawalAmount AS withdraw_amount,DepositAmount AS deposit_amount,TransactionNotes AS transaction_notes
FROM CustomerAccountDetail 
WHERE AccountId='LS0000001'
ORDER BY TransactionTime DESC
LIMIT 100;

use banking_system;
SELECT concat(FirstName,' ',c.LastName) FROM Customers c INNER JOIN CustomerAccountSummary cas ON c.CustomerId = cas.CustomerId ;

SELECT cad.DepositAmount - cad.WithdrawAmount AS current_balance,
cas.OriginalBalance AS orignal_balance,concat(FirstName,' ',c.LastName) AS customer_name
FROM CustomerAccountSummary cas INNER JOIN Customers c ON cas.CustomerId = c.CustomerId
INNER JOIN (SELECT AccountId,COALESCE(SUM(WithdrawalAmount),0.00) AS WithdrawAmount,
COALESCE(SUM(DepositAmount),0.00) AS DepositAmount
FROM 
CustomerAccountDetail GROUP BY AccountId) cad 
ON cas.AccountId = cad.AccountId AND cas.AccountId = 'AS00000004';

SELECT * FROM CustomerAccountSummary;

DELETE FROM CustomerAccountSummary WHERE AccountId ='AS00000006';

DROP TABLE CustomerAccountDetail;
DROP TABLE CustomerAccountSummary;
DROP TABLE CreditCardServices;
DROP TABLE LoanServices;
DROP TABLE AccountServices;
DROP TABLE Customers;
DROP TABLE Employees;

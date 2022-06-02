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

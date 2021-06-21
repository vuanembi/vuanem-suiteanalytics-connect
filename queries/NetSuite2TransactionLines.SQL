SELECT 
transaction AS TRANSACTION_ID,
expenseaccount AS ACCOUNT_ID,
netamount,
rate,
rateamount,
linelastmodifieddate,
ratepercent
FROM transactionLine

INSERT INTO STV2024101049__DWH.global_metrics (date_update, currency_from, cnt_transactions, amount_total, avg_transactions_per_account, cnt_accounts_make_transactions)
                WITH latest_transactions AS (
                    SELECT * 
                    FROM (
                        SELECT t.*, 
                            ROW_NUMBER() OVER (PARTITION BY operation_id ORDER BY transaction_dt DESC) AS row_num
                        FROM STV2024101049__STAGING.transactions t
                    ) sub
                    WHERE row_num = 1 
                    AND account_number_from > 0 
                    AND transaction_dt >= '{data_date}' 
                    AND transaction_dt < '{execution_date}'
                )
                SELECT 
                    DATE(transaction_dt) AS date_update,
                    lt.currency_code as currency_from, 
                    COUNT(*) AS cnt_transactions,
                    (SUM(amount * COALESCE(currency_code_div, 1))/100)::BIGINT AS amount_total,
                    ((SUM(amount) / NULLIF(COUNT(DISTINCT account_number_from), 0))/100)::BIGINT AS avg_transactions_per_account,
                    COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
                FROM latest_transactions lt
                LEFT JOIN STV2024101049__STAGING.currencies c  
                    ON c.currency_code = lt.currency_code 
                    AND DATE(lt.transaction_dt) = DATE(c.date_update) 
                    AND c.currency_code_with = 420
                GROUP BY lt.currency_code, DATE(transaction_dt);
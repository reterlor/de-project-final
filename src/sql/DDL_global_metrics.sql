CREATE TABLE STV2024101049__DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from VARCHAR(10) NOT NULL,
    amount_total BIGINT NOT NULL,
    cnt_transactions INT NOT NULL,
    avg_transactions_per_account BIGINT NOT NULL,
    cnt_accounts_make_transactions INT NOT NULL
)
ORDER BY date_update, currency_from
SEGMENTED BY hash(date_update) ALL NODES;
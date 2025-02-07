CREATE TABLE STV2024101049__STAGING.currencies (
    date_update DATE NOT NULL,
    currency_code INTEGER NOT NULL,
    currency_code_with INTEGER NOT NULL,
    currency_code_div NUMERIC(3,2) NOT NULL,
    PRIMARY KEY (date_update, currency_code, currency_code_with)
)
ORDER BY date_update
SEGMENTED BY hash(date_update) ALL NODES
PARTITION BY date_update;
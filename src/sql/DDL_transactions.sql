CREATE TABLE STV2024101049__STAGING.transactions (
    operation_id UUID NOT NULL,
    account_number_from BIGINT NOT NULL,
    account_number_to BIGINT NOT NULL,
    currency_code CHAR(3) NOT NULL,
    country VARCHAR(50) NOT NULL,
    status VARCHAR(20) CHECK (status IN ('queued', 'in_progress', 'blocked', 'done', 'chargeback')) NOT NULL,
    transaction_type VARCHAR(30) CHECK (transaction_type IN (
        'authorization',
        'sbp_outgoing',
        'sbp_incoming',
        'transfer_outgoing',
        'c2a_incoming',
        'transfer_incoming',
        'loyalty_cashback',
        'c2b_partner_incoming',
        'authorization_commission'
    )) NOT NULL,
    amount BIGINT NOT NULL,
    transaction_dt TIMESTAMP NOT NULL
)
ORDER BY transaction_dt, operation_id
SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES;
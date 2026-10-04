-- Service side of the transactional outbox for the lending service (PostgreSQL). Design: section 8.
-- An event exists if and only if the change committed: the business write and the outbox insert share one transaction.
-- sequence = the loan's own version, incremented under the row lock, so versions follow commit order per loan.
-- psql-style :named parameters; the service binds them (:principal_paise, :amount_paise, :loan_id, :payment_ref, ...).

create table outbox (
    id            uuid        primary key,
    aggregatetype text        not null,   -- routes the topic: lending.<aggregatetype>.events
    aggregateid   text        not null,   -- Kafka message key: one loan = one partition = ordered
    type          text        not null,   -- event type, e.g. EmiPaid
    payload       jsonb       not null,
    created_at    timestamptz not null default now()
);
-- Rows are deleted right after insert: Debezium reads the INSERT from the WAL, so the table stays empty.

begin;

insert into repayments (payment_ref, loan_id, amount_paise, principal_paise, paid_at)
values (:'payment_ref', :'loan_id', :amount_paise, :principal_paise, now());

-- the UPDATE takes the row lock on the loan; the CTE feeds its new state into the outbox row
with l as (
    update loans
       set outstanding_principal_paise = outstanding_principal_paise - :principal_paise,
           status = case when outstanding_principal_paise - :principal_paise = 0 then 'CLOSED' else status end,
           version = version + 1,
           updated_at = now()
     where loan_id = :'loan_id'
 returning loan_id, customer_id, lender_id, version, status, outstanding_principal_paise,
           next_emi_date, next_emi_paise, days_past_due
)
insert into outbox (id, aggregatetype, aggregateid, type, payload)
select e.event_id, 'loan', l.loan_id, 'EmiPaid',
       jsonb_build_object(
           'event_id',     e.event_id,          -- same uuid as the row id: the consumer's dedup key
           'event_type',   'EmiPaid',
           'aggregate_id', l.loan_id,
           'sequence',     l.version,
           'occurred_at',  now(),
           'customer_id',  l.customer_id,
           'data',  jsonb_build_object('amount_paise', :amount_paise, 'payment_ref', :'payment_ref', 'lender_id', l.lender_id),
           'state', jsonb_build_object('status', l.status,
                                       'outstanding_principal_paise', l.outstanding_principal_paise,
                                       'next_emi_date', l.next_emi_date,
                                       'next_emi_paise', l.next_emi_paise,
                                       'days_past_due', l.days_past_due))
  from l
 cross join lateral (select gen_random_uuid() as event_id) e
returning id \gset

delete from outbox where id = :'id';   -- the WAL already holds the insert

commit;

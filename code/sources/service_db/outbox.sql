-- Service side of the transactional outbox for the lending service (PostgreSQL). Design: section 8.
-- An event exists if and only if the change committed: the business write and the outbox insert share one transaction.
-- sequence = the loan's own version, incremented under the row lock, so versions follow commit order per loan.
-- psql-style :named parameters; the service binds them (:principal_paise, :amount_paise, :installment_no, :loan_id, :payment_ref, ...).
-- The full loan state, including the installment schedule, rides on every event (~1.5 KB for a 24-month loan),
-- so any consumer can rebuild the loan as of any event.

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

-- the service allocates the payment to the installment; the warehouse never re-implements the loan system
update installments
   set paid_paise    = paid_paise + :amount_paise,
       fully_paid_at = coalesce(fully_paid_at, case when paid_paise + :amount_paise >= emi_due_paise then now() end)
 where loan_id = :'loan_id' and installment_no = :installment_no;

-- the UPDATE takes the row lock on the loan; the CTE feeds its new state into the outbox row
with l as (
    update loans
       set outstanding_principal_paise = outstanding_principal_paise - :principal_paise,
           status = case when outstanding_principal_paise - :principal_paise = 0 then 'CLOSED' else status end,
           closed_at = case when outstanding_principal_paise - :principal_paise = 0 then now() else closed_at end,
           -- next EMI = the earliest installment still unpaid (reads the installment update above)
           next_emi_date  = (select min(due_date) from installments
                              where loan_id = :'loan_id' and fully_paid_at is null),
           next_emi_paise = (select emi_due_paise - paid_paise from installments
                              where loan_id = :'loan_id' and fully_paid_at is null
                              order by due_date limit 1),
           version = version + 1,
           updated_at = now()
     where loan_id = :'loan_id'
 returning *
)
insert into outbox (id, aggregatetype, aggregateid, type, payload)
select e.event_id, 'loan', l.loan_id, 'EmiPaid',
       jsonb_build_object(
           'event_id',     e.event_id,          -- same uuid as the row id: the consumer's dedup key
           'event_type',   'EmiPaid',
           'aggregate_id', l.loan_id,
           'sequence',     l.version,
           'occurred_at',  now(),
           -- the envelope above is the same for every topic; only this payload differs
           'payload', jsonb_build_object(
               'customer_id', l.customer_id,
               'data',  jsonb_build_object('amount_paise', :amount_paise, 'payment_ref', :'payment_ref',
                                           'installment_no', :installment_no, 'lender_id', l.lender_id),
               -- built explicitly (not to_jsonb(l)) so internal columns never leak into the contract
               'state', jsonb_build_object(
                            'loan_id', l.loan_id, 'application_id', l.application_id, 'customer_id', l.customer_id,
                            'lender_id', l.lender_id, 'product_code', l.product_code, 'status', l.status,
                            'principal_paise', l.principal_paise, 'processing_fee_paise', l.processing_fee_paise,
                            'net_disbursed_paise', l.net_disbursed_paise, 'interest_rate_bps', l.interest_rate_bps,
                            'apr_bps', l.apr_bps, 'tenure_months', l.tenure_months, 'emi_paise', l.emi_paise,
                            'disbursed_at', l.disbursed_at, 'first_emi_date', l.first_emi_date,
                            'maturity_date', l.maturity_date, 'closed_at', l.closed_at,
                            'written_off_at', l.written_off_at,
                            'outstanding_principal_paise', l.outstanding_principal_paise,
                            'next_emi_date', l.next_emi_date, 'next_emi_paise', l.next_emi_paise,
                            'schedule_version', l.schedule_version,
                            'installments', (
                                select jsonb_agg(jsonb_build_object(
                                           'installment_no', i.installment_no, 'due_date', i.due_date,
                                           'principal_due_paise', i.principal_due_paise,
                                           'interest_due_paise', i.interest_due_paise,
                                           'emi_due_paise', i.emi_due_paise, 'paid_paise', i.paid_paise,
                                           'fully_paid_at', i.fully_paid_at, 'bounce_count', i.bounce_count,
                                           'last_bounce_reason', i.last_bounce_reason) order by i.installment_no)
                                  from installments i where i.loan_id = l.loan_id))))
  from l
 cross join lateral (select gen_random_uuid() as event_id) e
returning id \gset

delete from outbox where id = :'id';   -- the WAL already holds the insert

commit;

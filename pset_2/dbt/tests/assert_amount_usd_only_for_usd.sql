-- Regla: amount_usd solo existe para awards en dólares y coincide con amount.
-- Evita sumar montos de monedas distintas como si fueran USD.
select award_key, amount, currency, amount_usd
from {{ ref('fact_awards') }}
where amount_usd is not null
  and (currency <> 'USD' or amount_usd <> amount)

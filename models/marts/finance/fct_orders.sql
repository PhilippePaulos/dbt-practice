with
    customers as (select * from {{ ref("stg_customers") }}),

    orders as (select * from {{ ref("stg_orders") }}),

    payments as (select * from {{ ref("stg_stripe__payment")}}),

    orders_payments as (
        select orders.order_id, orders.customer_id, payments.amount 
        from orders 
        join payments using(order_id)
    ),

    final as (
        select customers.customer_id, orders_payments.order_id, orders_payments.amount
        from orders_payments
        join customers on orders_payments.customer_id = customers.customer_id
    )

select * from final order by customer_id
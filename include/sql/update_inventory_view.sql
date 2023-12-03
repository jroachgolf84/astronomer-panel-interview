CREATE OR REPLACE VIEW daily_operations.items_to_order AS (
    SELECT
        *
    FROM daily_operations.inventory AS inventory
    WHERE inventory.inventory_date = '{{ ds }}'
        AND inventory.order_more = 1
);
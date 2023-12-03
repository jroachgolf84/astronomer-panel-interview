CREATE OR REPLACE VIEW daily_operations.top_selling_items_by_location AS (
    SELECT
        *
    FROM (
        SELECT
            item_sales.sales_date,
            item_sales.location_name,
            item_sales.item_name,
            rank() OVER (
                PARTITION BY
                    item_sales.sales_date,
                    item_sales.location_name
                ORDER BY
                    item_sales.quantity_sold DESC
            ) AS quantity_rank
        FROM daily_operations.item_sales AS item_sales
        WHERE item_sales.sales_date = '{{ ds }}'
    ) AS sub_query
    WHERE sub_query.quantity_rank = 1
);
CREATE OR REPLACE VIEW daily_operations.admission_by_entrance AS (
    SELECT
        admission_date,
        entrance_name,
        SUM(quantity_sold) AS total_quantity_sold,
        SUM(ticket_price * quantity_sold) AS total_gross_revenue
    FROM daily_operations.guest_attendance AS guest_attendance
    WHERE guest_attendance.admission_date = '{{ ds }}'
    GROUP BY
        admission_date,
        entrance_name
);
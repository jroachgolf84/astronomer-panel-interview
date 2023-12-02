CREATE OR REPLACE VIEW daily_operations.ytd_labor_metrics AS (
    SELECT
        labor.employee_name,
        SUM(labor.hours_worked) AS ytd_hours_worked,
        SUM(labor.pre_tax_earnings) AS ytd_pre_tax_earnings,
        SUM(labor.post_tax_earnings) AS ytd_post_tax_earnings
    FROM daily_operations.labor AS labor
    WHERE DATE(labor.labor_date) <= DATE('{{ ds }}')
    GROUP BY
        labor.employee_name
);
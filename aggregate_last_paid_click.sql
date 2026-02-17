WITH paid_sessions AS (
    SELECT 
        visitor_id,
        DATE(visit_date) AS visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign
    FROM sessions
    WHERE LOWER(medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
session_aggregation AS (
    SELECT 
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(*) AS visitors_count
    FROM paid_sessions
    GROUP BY 
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
),
ad_costs AS (
    SELECT 
        campaign_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent
        FROM vk_ads
        UNION ALL
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent
        FROM ya_ads
    ) all_ads
    GROUP BY 
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign
),
last_paid_click AS (
    SELECT 
        l.lead_id,
        l.visitor_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ps.visit_date AS attributed_visit_date,
        ps.utm_source,
        ps.utm_medium,
        ps.utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY l.lead_id 
            ORDER BY ps.visit_date DESC
        ) AS rn
    FROM leads l
    JOIN paid_sessions ps 
        ON l.visitor_id = ps.visitor_id
        AND ps.visit_date <= DATE(l.created_at)
),
lead_aggregation AS (
    SELECT 
        attributed_visit_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(*) AS leads_count,
        COUNT(CASE WHEN closing_reason = 'Успешно реализовано' OR status_id = 142 THEN 1 END) AS purchases_count,
        SUM(CASE WHEN closing_reason = 'Успешно реализовано' OR status_id = 142 THEN amount ELSE 0 END) AS revenue
    FROM last_paid_click
    WHERE rn = 1
    GROUP BY 
        attributed_visit_date,
        utm_source,
        utm_medium,
        utm_campaign
)
SELECT
    sa.visit_date,
    sa.visitors_count,
    sa.utm_source,
    sa.utm_medium,
    sa.utm_campaign,
    NULLIF(ac.total_cost, 0) AS total_cost,
    COALESCE(la.leads_count, 0) AS leads_count,
    COALESCE(la.purchases_count, 0) AS purchases_count,
    COALESCE(la.revenue, 0) AS revenue
FROM session_aggregation AS sa
LEFT JOIN ad_costs AS ac
    ON sa.visit_date = ac.visit_date
    AND sa.utm_source = ac.utm_source
    AND sa.utm_medium = ac.utm_medium
    AND sa.utm_campaign = ac.utm_campaign
LEFT JOIN lead_aggregation AS la
    ON sa.visit_date = la.visit_date
    AND sa.utm_source = la.utm_source
    AND sa.utm_medium = la.utm_medium
    AND sa.utm_campaign = la.utm_campaign
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 15;
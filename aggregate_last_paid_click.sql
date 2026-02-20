WITH buffer AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON s.visitor_id = l.visitor_id
        AND s.visit_date <= l.created_at
    WHERE LOWER(s.medium) IN (
        'cpc',
        'cpm',
        'cpa',
        'youtube',
        'cpp',
        'tg',
        'social'
    )
),

aggr_last AS (
    SELECT
        b.visit_date::DATE AS visit_date,
        b.utm_source,
        b.utm_medium,
        b.utm_campaign,
        COUNT(b.visitor_id) AS visitors_count,
        COUNT(b.lead_id) AS leads_count,
        COUNT(
            CASE
                WHEN b.closing_reason = 'Успешно реализовано'
                    OR b.status_id = 142
                THEN b.visitor_id
            END
        ) AS purchases_count,
        SUM(
            CASE
                WHEN b.closing_reason = 'Успешно реализовано'
                    OR b.status_id = 142
                THEN b.amount
            END
        ) AS revenue
    FROM buffer AS b
    WHERE b.rn = 1
    GROUP BY
        b.visit_date::DATE,
        b.utm_source,
        b.utm_medium,
        b.utm_campaign
),

ads_costs AS (
    SELECT
        va.campaign_date::DATE AS campaign_date,
        va.utm_source,
        va.utm_medium,
        va.utm_campaign,
        SUM(va.daily_spent) AS total_cost
    FROM vk_ads AS va
    GROUP BY
        va.campaign_date::DATE,
        va.utm_source,
        va.utm_medium,
        va.utm_campaign
    UNION
    SELECT
        ya.campaign_date::DATE AS campaign_date,
        ya.utm_source,
        ya.utm_medium,
        ya.utm_campaign,
        SUM(ya.daily_spent) AS total_cost
    FROM ya_ads AS ya
    GROUP BY
        ya.campaign_date::DATE,
        ya.utm_source,
        ya.utm_medium,
        ya.utm_campaign
)

SELECT
    al.visit_date,
    al.visitors_count,
    al.utm_source,
    al.utm_medium,
    al.utm_campaign,
    ac.total_cost,
    al.leads_count,
    al.purchases_count,
    al.revenue
FROM aggr_last AS al
LEFT JOIN ads_costs AS ac
    ON al.visit_date = ac.campaign_date
    AND al.utm_source = ac.utm_source
    AND al.utm_medium = ac.utm_medium
    AND al.utm_campaign = ac.utm_campaign
ORDER BY
    al.revenue DESC NULLS LAST,
    al.visit_date ASC,
    al.visitors_count DESC,
    al.utm_source ASC,
    al.utm_medium ASC,
    al.utm_campaign ASC
LIMIT 15;

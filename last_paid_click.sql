WITH paid_sessions AS (
    SELECT
        visitor_id,
        visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign
    FROM sessions
    WHERE
        LOWER(medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

lead_attribution AS (
    SELECT
        l.lead_id,
        l.visitor_id,
        l.created_at AS lead_created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        MAX(ps.visit_date) AS attributed_visit_date
    FROM leads AS l
    INNER JOIN paid_sessions AS ps
        ON l.visitor_id = ps.visitor_id
    WHERE ps.visit_date <= l.created_at
    GROUP BY
        l.lead_id,
        l.visitor_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
)

SELECT
    s.visitor_id,
    s.visit_date,
    s.source AS utm_source,
    s.medium AS utm_medium,
    s.campaign AS utm_campaign,
    la.lead_id,
    la.lead_created_at AS created_at,
    la.amount,
    la.closing_reason,
    la.status_id
FROM sessions AS s
LEFT JOIN lead_attribution AS la
    ON
        s.visitor_id = la.visitor_id
        AND s.visit_date = la.attributed_visit_date
ORDER BY
    la.amount DESC NULLS LAST,
    s.visit_date ASC,
    s.source ASC,
    s.medium ASC,
    s.campaign ASC;

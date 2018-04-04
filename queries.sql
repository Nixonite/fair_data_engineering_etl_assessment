CREATE MATERIALIZED VIEW recip_contributions_agg
AS

    SELECT
        contrib.contributor_id,
        contrib.recipient_id,
        contrib.gender,
        contrib.cycle,
        CASE WHEN contrib.recipient_id LIKE 'N%' THEN 'candidate'
            ELSE 'committee'
        END AS recipient_type,
        COALESCE(SUM(contrib.amount),0) AS total_donated
    FROM individual_contributions AS contrib
    GROUP BY
        contrib.contributor_id,
        contrib.recipient_id,
        contrib.gender,
        contrib.cycle
WITH DATA;

CREATE MATERIALIZED VIEW first_time_contrib
AS

    WITH partitioned AS (
        SELECT 
            contrib.contributor_id,
            contrib.recipient_id,
            contrib.cycle,
            contrib.amount,
            RANK() OVER
                (PARTITION BY contrib.contributor_id ORDER BY contrib.date ASC) as nth_donation
        FROM individual_contributions as contrib
    )
    SELECT
        COUNT(contributor_id) as num_contrib,
        recipient_id,
        cycle,
        SUM(amount) AS total_donated,
        ROUND(AVG(amount)) AS avg_donated
    FROM partitioned
    WHERE nth_donation = 1
    GROUP BY
        recipient_id,
        cycle

WITH DATA;


CREATE MATERIALIZED VIEW general_reporting
AS

    SELECT
        contrib.recipient_id,
        contrib.cycle,
        COUNT(contrib.contributor_id) AS num_contributors,
        SUM(contrib.total_donated) AS total_donated,
        ROUND(SUM(contrib.total_donated)/COUNT(*)) AS avg_donation,
        COALESCE(ftd.num_contrib,0) AS first_time_num_contrib,
        COALESCE(ftd.avg_donated,0) AS first_time_avg_contrib,
        COALESCE(ftd.total_donated,0) AS first_time_total_contrib,
        COUNT(CASE WHEN contrib.gender = 'M' THEN
            1 END) as male_num_contrib,
        COUNT(CASE WHEN contrib.gender = 'F' THEN
            1 END) as female_num_contrib,
        COALESCE(SUM(CASE WHEN contrib.gender = 'M' THEN
            contrib.total_donated END),0) as male_total_contrib,
        COALESCE(SUM(CASE WHEN contrib.gender = 'F' THEN
            contrib.total_donated END),0) as female_total_contrib,
        ROUND(COALESCE(AVG(CASE WHEN contrib.gender = 'M' THEN
            contrib.total_donated END),0)) as male_avg_contrib,
        ROUND(COALESCE(AVG(CASE WHEN contrib.gender = 'F' THEN
            contrib.total_donated END),0)) as female_avg_contrib,
        COUNT(CASE WHEN contrib.total_donated < 100 THEN
            1 END) as num_small_contrib,
        COUNT(CASE WHEN contrib.total_donated > 2000 THEN
            1 END) as num_large_contrib,
        COALESCE(SUM(CASE WHEN contrib.total_donated < 100 THEN
            contrib.total_donated END),0) as small_contrib_total,
        COALESCE(SUM(CASE WHEN contrib.total_donated > 2000 THEN
            contrib.total_donated END),0) as large_contrib_total,
        ROUND(COALESCE(AVG(CASE WHEN contrib.total_donated < 100 THEN
            contrib.total_donated END),0)) as avg_small_contrib,
        ROUND(COALESCE(AVG(CASE WHEN contrib.total_donated > 2000 THEN
            contrib.total_donated END),0)) as avg_large_contrib
    FROM recip_contributions_agg AS contrib
    LEFT JOIN first_time_contrib AS ftd 
        ON contrib.recipient_id = ftd.recipient_id
        AND contrib.cycle = ftd.cycle
    GROUP BY
    contrib.recipient_id,
    contrib.cycle,
    first_time_num_contrib,
    first_time_avg_contrib,
    first_time_total_contrib

WITH DATA;
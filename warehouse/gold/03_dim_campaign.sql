-- dim_campaign: from simulator.campaigns.CAMPAIGNS, rendered as a VALUES list (campaigns_values), no history.
-- name is the utm_campaign; start_date and end_date stay NULL (inserted NULL, never overwritten).
-- The unknown member (key -1, campaign_id unknown) is seeded by apply_ddl and never matches a campaign here.
MERGE INTO {gold}.dim_campaign AS t
USING (
  SELECT campaign_id, utm_campaign, platform, name, start_date, end_date
  FROM VALUES {campaigns_values} AS v(campaign_id, utm_campaign, platform, name, start_date, end_date)
) AS s
ON t.campaign_id = s.campaign_id
WHEN MATCHED AND (NOT (t.utm_campaign <=> s.utm_campaign) OR NOT (t.platform <=> s.platform)
                  OR NOT (t.name <=> s.name)) THEN
  UPDATE SET t.utm_campaign = s.utm_campaign, t.platform = s.platform, t.name = s.name
WHEN NOT MATCHED THEN
  INSERT (campaign_id, utm_campaign, platform, name, start_date, end_date)
  VALUES (s.campaign_id, s.utm_campaign, s.platform, s.name, s.start_date, s.end_date);

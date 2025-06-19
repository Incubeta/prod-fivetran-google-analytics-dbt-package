with ga_table_1 as (
  select
    -- Fivetran fields
    safe_cast(_fivetran_id as string) as _fivetran_id,
    safe_cast(_fivetran_synced as timestamp) as _fivetran_synced,
    safe_cast(property as string) as property_name,
    -- Dimmensions
    safe_cast(country as string) as country,
    safe_cast(date as date) as date,
    safe_cast(event_name as string) as event_name,
    safe_cast(session_campaign_name as string) as session_campaign_name,
    safe_cast(session_default_channel_group as string) as session_default_channel_group,
    safe_cast(session_manual_ad_content as string) as session_manual_ad_content,
    safe_cast(session_source_medium as string) as session_source_medium,
    -- Metrics
    safe_cast(add_to_carts as FLOAT64) as add_to_carts,
    safe_cast(bounce_rate as FLOAT64) as bounce_rate,
    safe_cast(engaged_sessions as INT64) as engaged_sessions,
    safe_cast(event_value as FLOAT64) as event_value,
    safe_cast(new_users as INT64) as new_users,
    safe_cast(sessions as INT64) as sessions,
    safe_cast(user_engagement_duration as FLOAT64) as user_engagement_duration
  from {{ source('ga4', 'ga4_channel_performance_v_1') }}
  {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses >= to include records arriving later on the same day as the last run of this model)
    where date >= (select coalesce(date_sub(max(date), interval 1 day), '1900-01-01') from {{ this }})

    {% endif %}
),

ga_table_2 as (
  select
    -- Fivetran fields
    safe_cast(_fivetran_id as string) as _fivetran_id,
    safe_cast(_fivetran_synced as timestamp) as _fivetran_synced,
    safe_cast(property as string) as property_name,
    -- Dimmensions
    safe_cast(country as string) as country,
    safe_cast(date as date) as date,
    safe_cast(event_name as string) as event_name,
    safe_cast(session_campaign_name as string) as session_campaign_name,
    safe_cast(session_default_channel_group as string) as session_default_channel_group,
    safe_cast(session_manual_ad_content as string) as session_manual_ad_content,
    safe_cast(session_source_medium as string) as session_source_medium,
    -- Metrics
    safe_cast(key_events as INT64) as key_events,
    safe_cast(tax_amount as FLOAT64) as tax_amount,
    safe_cast(event_count as INT64) as event_count,
    safe_cast(total_revenue as FLOAT64) as total_revenue,
    safe_cast(transactions as INT64) as transactions,
    safe_cast(shipping_amount as FLOAT64) as shipping_amount
  from {{ source('ga4', 'ga4_channel_performance_v_2') }}
  {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses >= to include records arriving later on the same day as the last run of this model)
    where date >= (select coalesce(date_sub(max(date), interval 1 day), '1900-01-01') from {{ this }})

  {% endif %}
),

accounts as (
  select
    name as account_name,
    display_name as account_displayName 
  from {{ source('ga4', 'accounts') }}
),

properties as (
  select
    account as account_name,
    name as property_name,
    parent as property_parent,
    display_name as property_displayName,
    currency_code as property_currencyCode
  from {{ source('ga4', 'properties') }}
),

final as (
  select
    coalesce(ga_table_1._fivetran_id, ga_table_2._fivetran_id) as _fivetran_id,
    coalesce(ga_table_1._fivetran_synced, ga_table_2._fivetran_synced) as _fivetran_synced,
    coalesce(ga_table_1.property_name, ga_table_2.property_name) as property_name,
    country,
    date,
    event_name as eventName,
    session_campaign_name as sessionCampaignName,
    session_default_channel_group as sessionDefaultChannelGroup,
    session_manual_ad_content as sessionManualAdContent,
    session_source_medium as sessionSourceMedium,
    replace(coalesce(ga_table_1.property_name, ga_table_2.property_name), 'properties/', '') as propertyId,
    account_name,
    account_displayName,
    property_parent,
    property_displayName,
    property_currencyCode,
    add_to_carts as addToCarts,
    bounce_rate as bounceRate,
    key_events as keyEvents,
    engaged_sessions as engagedSessions,
    event_count as eventCount,
    event_value as eventValue,
    new_users as newUsers,
    sessions as sessions,
    shipping_amount as shippingAmount,
    tax_amount as taxAmount,
    total_revenue as totalRevenue,
    transactions as transactions,
    user_engagement_duration as userEngagementDuration
  from
    ga_table_1
  full outer join
    ga_table_2
  using (country, 
        date, 
        event_name, 
        session_campaign_name, 
        session_default_channel_group, 
        session_manual_ad_content, 
        session_source_medium)
    left join properties on coalesce(ga_table_1.property_name, ga_table_2.property_name) = properties.property_name
    left join accounts using (account_name)
    )

select * from final
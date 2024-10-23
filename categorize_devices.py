import boto3

def run_athena_query():
    client = boto3.client('athena', region_name='us-east-1')

    query = """
    WITH CommissionedDevicesList AS (
    -- Select all commissioned devices from the 'custexp_list_devices' table
    SELECT id, device_group_id
    FROM dynamodb.default.custexp_list_devices
),
CommissioningDates AS (
    -- Convert 'timestamp' field (date in 'YYYY-MM-DD') to a proper date type
    SELECT id, 
           CAST("timestamp" AS date) AS commissioning_date  -- Cast timestamp varchar to date
    FROM dynamodb.default.custexp_commission_dates
),
CommissionedDevices AS (
    -- Join commissioned devices list with their commissioning dates
    SELECT
        cdl.id,
        cdl.device_group_id,
        cd.commissioning_date
    FROM
        CommissionedDevicesList cdl
    LEFT JOIN
        CommissioningDates cd
    ON
        cdl.id = cd.id
),
DeviceTimes AS (
    -- Convert dTm to a readable timestamp and join with commissioned devices
    SELECT
        t.dId,
        date(from_unixtime(CAST(t.dTm AS BIGINT))) AS readable_dTm,  -- Extract only the date from the timestamp
        c.commissioning_date
    FROM
        "mi_dev_spi_moe_mo1_stats_daily" t
    LEFT JOIN
        CommissionedDevices c
    ON
        t.dId = c.id  -- Join commissioned devices with main data on dId
),
AggregatedTimes AS (
    -- Find the latest dTm for each device, ensuring it is after the commissioning date if present
    SELECT
        dId,
        MAX(CASE
            -- Case 1: We have both dTm and commissioning date, and dTm is after commissioning date
            WHEN readable_dTm IS NOT NULL AND commissioning_date IS NOT NULL AND readable_dTm >= commissioning_date THEN readable_dTm
            -- Case 2: We have dTm but no commissioning date, categorize based on dTm
            WHEN readable_dTm IS NOT NULL AND commissioning_date IS NULL THEN readable_dTm
            -- Case 3: We have no dTm, so no value for this device in this case
            ELSE NULL
        END) AS latest_dTm,
        commissioning_date
    FROM
        DeviceTimes
    WHERE
        -- Ensure the dTm is not after the current date
        (readable_dTm <= current_date OR readable_dTm IS NULL)
    GROUP BY
        dId, commissioning_date
),
DeviceStatuses AS (
    -- Categorize each device based on dTm or commissioning date if dTm is missing
    SELECT
        cd.id AS dId,
        COALESCE(agg.latest_dTm, cd.commissioning_date) AS reference_date,
        CASE
            -- Case 1: We have a valid dTm after the commissioning date
            WHEN agg.latest_dTm IS NOT NULL THEN
                CASE
                    WHEN agg.latest_dTm >= current_date - INTERVAL '8' DAY THEN 'Green'
                    WHEN agg.latest_dTm BETWEEN current_date - INTERVAL '38' DAY AND current_date - INTERVAL '9' DAY THEN 'Yellow'
                    ELSE 'Red'
                END
            -- Case 3: No dTm but we have a commissioning date
            WHEN cd.commissioning_date IS NOT NULL AND agg.latest_dTm IS NULL THEN
                CASE
                    WHEN cd.commissioning_date >= current_date - INTERVAL '8' DAY THEN 'Green'
                    WHEN cd.commissioning_date BETWEEN current_date - INTERVAL '38' DAY AND current_date - INTERVAL '9' DAY THEN 'Yellow'
                    ELSE 'Red'
                END
            -- Case 4: No dTm and no commissioning date
            ELSE
                'Red'
        END AS device_status
    FROM
        CommissionedDevices cd
    LEFT JOIN
        AggregatedTimes agg ON cd.id = agg.dId
),
StatusCounts AS (
    -- Count the number of devices in each status category
    SELECT
        device_status,
        COUNT(*) AS count,
        ARRAY_AGG(dId) AS device_ids
    FROM
        DeviceStatuses
    GROUP BY
        device_status
),
TotalDevices AS (
    -- Get the total number of commissioned devices
    SELECT
        COUNT(*) AS total_count
    FROM
        DeviceStatuses
)
-- Final output: device status and percentages
SELECT
    sc.device_status,
    ROUND((sc.count * 100.0 / td.total_count), 2) AS percentage,
    sc.count,
    sc.device_ids,
    td.total_count
FROM
    StatusCounts sc
CROSS JOIN
    TotalDevices td
ORDER BY
    sc.device_status
    """

    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Catalog": "dynamodb",
                "Database": "default"
            },
            ResultConfiguration={
                'OutputLocation': 's3://romet-dailystatus-bucket/'
            }
        )
        print(f"Query started: {response['QueryExecutionId']}")
    except Exception as e:
        print(f"Error executing query: {e}")

if __name__ == "__main__":
    run_athena_query()

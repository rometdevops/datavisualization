import boto3

# Function to dynamically fetch a list of required DynamoDB tables
def get_required_table_list():
    # Instead of listing all tables, specify the ones of interest
    table_list = ['mi_dev_spi_moe_mo1_stats_daily','mi_dev_onegascom_kgsuti_kgsdg_stats_daily', 'mi_dev_onegascom_onguti_ongdg_stats_daily', 'mi_dev_onegascom_tgsuti_tgsdg_stats_daily','mi_dev_spi_alb_ala_stats_daily','mi_dev_spi_mow_mow_stats_daily','mi_dev_spi_sput_spiredg_stats_daily',]
    return table_list

# Function to run Athena query for a specific table
def run_athena_query(table_name, output_location, database='default', region='us-east-1'):
    client = boto3.client('athena', region_name=region)

    # The stats table name is constructed dynamically based on the main table name
    stats_table_name = f"{table_name}"

    query = f"""
    WITH CommissionedDevicesList AS (
        -- Select all commissioned devices from the 'custexp_list_devices' table
        SELECT id, device_group_id
        FROM dynamodb.default.custexp_list_devices where device_group_id = 'mi_dev_spi_moe_mo1'
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
            --COALESCE(agg.latest_dTm, cd.commissioning_date) AS reference_date,
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
    )--,

    select * from DeviceStatuses


    """

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database, 'Catalog': 'dynamodb'},
        ResultConfiguration={'OutputLocation': output_location}
    )
    return response['QueryExecutionId']

# Main script execution
if __name__ == "__main__":
    # Fetch list of required DynamoDB tables
    table_list = get_required_table_list()
    output_location = 's3://romet-dailystatus-bucket/'

    # Iterate over each table and run the Athena query
    for table in table_list:
        query_id = run_athena_query(table_name=table, output_location=output_location)
        print(f"Query started for {table}: {query_id}")

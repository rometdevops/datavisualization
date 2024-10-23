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
        SELECT id, device_group_id FROM dynamodb.default.custexp_list_devices WHERE device_group_id='mi_dev_spi_moe_mo1'
    ),
    CommissioningDates AS (
        SELECT id, CAST("timestamp" AS date) AS commissioning_date FROM dynamodb.default.custexp_commission_dates
    ),
    CommissionedDevices AS (
        SELECT cdl.id, cdl.device_group_id, cd.commissioning_date
        FROM CommissionedDevicesList cdl
        LEFT JOIN CommissioningDates cd ON cdl.id = cd.id
    ),
    DeviceTimes AS (
        -- Use the dynamic stats table from the table list
        SELECT t.dId, date(from_unixtime(CAST(t.dTm AS BIGINT))) AS readable_dTm, 
               c.commissioning_date
        FROM dynamodb.default.{stats_table_name} t
        LEFT JOIN CommissionedDevices c ON t.dId = c.id
    ),
    AggregatedTimes AS (
        SELECT dId, MAX(readable_dTm) AS latest_dTm
        FROM DeviceTimes
        GROUP BY dId
    ),
    DeviceStatuses AS (
        SELECT dId, latest_dTm,
        CASE
            WHEN latest_dTm >= current_date - INTERVAL '7' DAY THEN 'Green'
            WHEN latest_dTm BETWEEN current_date - INTERVAL '37' DAY AND current_date - INTERVAL '8' DAY THEN 'Yellow'
            ELSE 'Red'
        END AS device_status
        FROM AggregatedTimes
    ),
    StatusCounts AS (
        SELECT device_status, COUNT(*) AS count
        FROM DeviceStatuses
        GROUP BY device_status
    ),
    TotalDevices AS (
        SELECT COUNT(*) AS total_count
        FROM DeviceStatuses
    )
    SELECT sc.device_status, ROUND((sc.count * 100.0 / td.total_count), 2) AS percentage,
     sc.count,
     td.total_count
    FROM StatusCounts sc
    CROSS JOIN TotalDevices td
    ORDER BY sc.device_status;
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


from datetime import datetime, timedelta
import duckdb
import os

from scripts import utils

DB_PATH = utils.config.get("paths").get("db_path")

table_yellow_trip = (
    utils.source_to_tablename(
        utils.config.get("sources").get("urls")
            .get("trip_record_format").format(
            list(utils.config.get("sources").get("vehicles"))[0], "23-01"
        )
    )
)

# table names
# todo softcode dari utils.config
raw_tablename = 'yellow_tripdata'
raw_schema = 'raw'
fact_tablename = 'fct_trips'
fact_schema = 'analytics_analytics'
staging_tablename = 'stg_taxi_trips'
staging_schema = 'analytics_staging'
dim_zones_tablename = 'dim_zones'
agg_daily_tablename = 'agg_daily_stats'

ALERT_THRESHOLDS = {
    'min_daily_trips': 10000,
    'max_data_age_hours': 48,
    'data_loss': 20,  # percent
}

def check_data_freshness(**context):
    """Check apakah data up to date, dibandingkan dengan 
    execution_date dari context Airflow (bukan date time now)
    """
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    try:
        # Get execution date from context
        execution_date = context['execution_date']
        execution_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S')
        
        # data terakhir di fct_trips
        query = f"""
        SELECT 
            MAX(pickup_datetime) as latest_pickup,
            COUNT(*) as total_trips,
            DATEDIFF(
                'hour', MAX(pickup_datetime),
                TIMESTAMP '{execution_date_str}'
            ) as hours_old
        FROM {fact_schema}.{fact_tablename}
        """
        result = conn.execute(query).fetchone()
        
        if result:
            latest_pickup, total_trips, hours_old = result
            
            freshness_status = {
                'latest_pickup': (
                    str(latest_pickup) if latest_pickup else 'No data'
                ),
                'total_trips': total_trips,
                'hours_old': hours_old if hours_old else 0,
                'execution_date': execution_date_str,
                'is_fresh': (
                    hours_old < ALERT_THRESHOLDS['max_data_age_hours']
                    if hours_old else False
                ),
                'status': (
                    'OK' if (
                        hours_old and
                        hours_old < ALERT_THRESHOLDS['max_data_age_hours']
                    ) else 'STALE'
                )
            }
        else:
            freshness_status = {
                'status': 'NO_DATA',
                'total_trips': 0,
                'is_fresh': False,
                'execution_date': execution_date_str
            }
        
        # push ke xcom selanjutnya
        context['task_instance'].xcom_push(
            key='freshness', value=freshness_status)
        
        return freshness_status
        
    finally:
        conn.close()


def validate_row_counts(**context):
    """validate row counts"""
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    try:
        # Get row counts from all layers
        counts_query = f"""
        SELECT 
            'raw' as layer,
            '{raw_tablename}' as table_name,
            COUNT(*) as row_count
        FROM {raw_schema}.{raw_tablename}
        
        UNION ALL
        
        SELECT 
            'staging' as layer,
            '{staging_tablename}' as table_name,
            COUNT(*) as row_count
        FROM {staging_schema}.{staging_tablename}
        
        UNION ALL
        
        SELECT 
            'marts' as layer,
            '{fact_tablename}' as table_name,
            COUNT(*) as row_count
        FROM {fact_schema}.{fact_tablename}
        
        UNION ALL
        
        SELECT 
            'marts' as layer,
            '{dim_zones_tablename}' as table_name,
            COUNT(*) as row_count
        FROM {fact_schema}.{dim_zones_tablename}
        
        UNION ALL
        
        SELECT 
            'marts' as layer,
            '{agg_daily_tablename}' as table_name,
            COUNT(*) as row_count
        FROM {fact_schema}.{agg_daily_tablename}
        """
        
        results = conn.execute(counts_query).fetchdf()
        
        # Ke bentuk dict, untuk xcom
        counts_data = results.to_dict('records')
        
        # data quality metrics
        raw_count = results[
                results['table_name'] == raw_tablename
            ]['row_count'].values[0]
        staging_count = results[
                results['table_name'] == staging_tablename
            ]['row_count'].values[0]
        marts_count = results[
                results['table_name'] == fact_tablename
            ]['row_count'].values[0]

        # presentase data loss dari raw ke staging, staging ke marts
        staging_loss = (
            ((raw_count - staging_count) / raw_count * 100)
            if raw_count > 0 else 0
        )
        marts_loss = (
            ((staging_count - marts_count) / staging_count * 100)
            if staging_count > 0 else 0
        )
        
        quality_metrics = {
            'raw_count': int(raw_count),
            'staging_count': int(staging_count),
            'marts_count': int(marts_count),
            'staging_loss_pct': round(staging_loss, 2),
            'marts_loss_pct': round(marts_loss, 2),
            'total_loss_pct': (
                round((raw_count - marts_count) / raw_count * 100, 2)
                if raw_count > 0 else 0
            )
        }
        
        validation_result = {
            'counts': counts_data,
            'quality_metrics': quality_metrics,
            'status': 'OK'
        }
        
        context['task_instance'].xcom_push(
            key='row_counts', value=validation_result)
        
        return validation_result
        
    finally:
        conn.close()


def compare_historical_patterns(**context):
    """compare pekan sekarang dengan pekan lalu"""
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    try:
        # last 7 days vs previous 7 days
        comparison_query = f"""
        WITH ranked_days AS (
            SELECT 
                trip_date,
                total_trips,
                ROW_NUMBER() OVER (ORDER BY trip_date DESC) as row_n
            FROM {fact_schema}.{agg_daily_tablename}
            LIMIT 14
        )
        SELECT 
            AVG(CASE WHEN row_n <= 7 THEN total_trips END) as current_avg_trips,
            AVG(CASE WHEN row_n > 7 THEN total_trips END) as previous_avg_trips
        FROM ranked_days
        """
        
        result = conn.execute(comparison_query).fetchone()
        
        if result and result[0] and result[1]:
            current = round(result[0], 0)
            previous = round(result[1], 0)
            change_pct = round((current - previous) / previous * 100, 2)
            
            comparison = {
                'current_avg_trips': current,
                'previous_avg_trips': previous,
                'trips_change_pct': change_pct,
                'status': 'OK'
            }
        else:
            comparison = {'status': 'NO_DATA', 'message': 'Insufficient historical data'}
        
        context['task_instance'].xcom_push(key='historical_comparison', value=comparison)
        
        return comparison
        
    finally:
        conn.close()


def detect_anomalies(**context):
    """Detect anomalies """
    
    # xcom dari task sebelumnya
    freshness = context['task_instance'].xcom_pull(task_ids='check_freshness', key='freshness')
    row_counts = context['task_instance'].xcom_pull(task_ids='validate_counts', key='row_counts')
    comparison = context['task_instance'].xcom_pull(task_ids='compare_patterns', key='historical_comparison')
    
    anomalies = []
    
    # Check freshness
    if freshness and not freshness.get('is_fresh', False):
        anomalies.append({
            'type': 'STALE_DATA',
            'severity': 'HIGH',
            'message': (
                f"Umur data mart terakhir vs data execution ini: "
                f"{freshness.get('hours_old', 'unknown')} jam "
                f"(threshold: {ALERT_THRESHOLDS['max_data_age_hours']}h)"
            )
        })
    
    # Check row count (data loss)
    if row_counts and row_counts.get('quality_metrics'):
        metrics = row_counts['quality_metrics']
        if metrics.get('staging_loss_pct', 0) > ALERT_THRESHOLDS['data_loss']:
            anomalies.append({
                'type': 'HIGH_DATA_LOSS',
                'severity': 'MEDIUM',
                'message': (
                    f"Staging layer losing {metrics['staging_loss_pct']}% "
                    f"of raw data (> {ALERT_THRESHOLDS['data_loss']}% threshold)"
                )
            })
    
    # Check historical pattern (trip volume)
    if comparison and comparison.get('current_avg_trips'):
        if comparison['current_avg_trips'] < ALERT_THRESHOLDS['min_daily_trips']:
            anomalies.append({
                'type': 'LOW_VOLUME',
                'severity': 'HIGH',
                'message': (
                    f"Daily trips ({comparison['current_avg_trips']}) "
                    f"di bawah threshold ({ALERT_THRESHOLDS['min_daily_trips']})"
                )
            })
    
    anomaly_result = {
        'anomalies': anomalies,
        'count': len(anomalies),
        'status': 'ANOMALIES_DETECTED' if anomalies else 'OK'
    }
    
    context['task_instance'].xcom_push(key='anomalies', value=anomaly_result)
    
    return anomaly_result


def generate_summary_report(**context):
    """Generate comprehensive daily summary report"""
    
    # Get all monitoring data
    freshness = context['task_instance'].xcom_pull(
        task_ids='check_freshness', key='freshness')
    row_counts = context['task_instance'].xcom_pull(
        task_ids='validate_counts', key='row_counts')
    comparison = context['task_instance'].xcom_pull(
        task_ids='compare_patterns', key='historical_comparison')
    anomalies = context['task_instance'].xcom_pull(
        task_ids='detect_anomalies', key='anomalies')

    # Data freshness
    report_html = f"""
    <h2>NYC Taxi Data Monitoring Report</h2>
    <p>DAG Date: {context['task_instance_key_str']}</p>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    
    <h3>Data Freshness</h3>
    <ul>
        <li>Status: <strong>{freshness.get('status', 'UNKNOWN')}</strong></li>
        <li>Data terakhir: {freshness.get('latest_pickup', 'N/A')}</li>
        <li>Data Age vs Execution date ini:
            {freshness.get('hours_old', 'N/A')} hours</li>
        <li>Banyak Trips total: {freshness.get('total_trips', 0):,}</li>
    </ul>
    """
    
    # Row counts & quality metrics
    if row_counts and row_counts.get('counts'):
        report_html += """
        <h3>Row Counts & Quality Metrics</h3>
        <ul>
        """
        for row in row_counts['counts']:
            report_html += (
                f"<li>"
                    f"{row['table_name']} ({row['layer']}): "
                    f"{row['row_count']:,} rows"
                "</li>"
            )
        report_html += "</ul>"

    if row_counts and row_counts.get('quality_metrics'):
        qm = row_counts['quality_metrics']
        report_html += (f"""<p>
            <h3>Data Loss:</h3>
            <p>Raw ke Staging: {qm.get('staging_loss_pct', 0)}%</p>
            <p>Staging ke Marts: {qm.get('marts_loss_pct', 0)}%</p>
            <p>Total: {qm.get('total_loss_pct', 0)}%</p>
        </p>""")
    
    # Historical comparison
    if comparison and comparison.get('status') == 'OK':
        report_html += f"""
        <h3>Historical Comparison (Pekan ini vs pekan lalu)</h3>
        <ul>
            <li>Rerata trips perhari pekan ini: {
                comparison.get('current_avg_trips', 0):,.0f}</li>
            <li>Rerata trips perhari pekan lalu: {
                comparison.get('previous_avg_trips', 0):,.0f}</li>
            <li>Perubahan: {
                comparison.get('trips_change_pct', 0):+.1f}%</li>
        </ul>
        """
    
    # Anomalies
    if anomalies and anomalies.get('count', 0) > 0:
        report_html += """
        <h3>Anomalies Detected</h3>
        <ul>"""
        for anomaly in anomalies.get('anomalies', []):
            report_html += (f"""<li>
                [{anomaly['severity']}] {anomaly['type']}: {anomaly['message']}
            </li>""")
        report_html += "</ul>"
    else:
        report_html += "<h3>No anomalies detected.</h3>"
    
    context['task_instance'].xcom_push(key='summary_report', value=report_html)

    return {
        'anomaly_count': anomalies.get('count', 0) if anomalies else 0
    }


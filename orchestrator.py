from dagster import asset, define_asset_job, ScheduleDefinition, Definitions
import subprocess
import os

# 1. Define the Asset
# This tells Dagster: "There is a data asset called 'sales_report'. 
# To update it, run this function."
@asset
def sales_report():
    # This runs 'dbt run' inside the 'retail_transform' folder
    # check=True means it will fail if dbt fails
    subprocess.run(["dbt", "run"], cwd="retail_transform", check=True)

# 2. Define the Job
# A job is a collection of assets to update.
sales_job = define_asset_job(name="update_sales_job", selection=[sales_report])

# 3. Define the Schedule
# This creates a schedule to run the job every minute ("* * * * *")
# In real life, you might do "0 * * * *" for hourly.
minute_schedule = ScheduleDefinition(
    job=sales_job,
    cron_schedule="* * * * *", 
)

# 4. Bundle it all together
defs = Definitions(
    assets=[sales_report],
    schedules=[minute_schedule]
)
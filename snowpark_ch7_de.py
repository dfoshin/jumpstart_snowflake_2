from snowflake.snowpark import Session
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import StringType
from snowflake.snowpark.functions import sum, col

def initiateSession():
    connection_parameters = {
        "account": "my_account",
        "user": "my_user",
        "password": "my_password",
        "role": "Accountadmin",
        "warehouse": "compute_wh",
        "database": "snowpark_ch7",
        "schema": "output"
    }
    session = Session.builder.configs(connection_parameters).create()
    return session


session = initiateSession()

print(session.sql("SELECT current_version()").collect())  # Verify connection


# Sample DataFrame
df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER")
df.show()

# Filter customers with account balance greater than 5000 and less than 10000
filtered_df = df.filter((df["C_ACCTBAL"] > 5000) & (df["C_ACCTBAL"] < 10000))
filtered_df.show()

# Group the data by nation key and compute the average account balance
aggregated_df = filtered_df.group_by("C_NATIONKEY").agg({"C_ACCTBAL": "avg"})
aggregated_df.show()

# Order the data by average account balance in descending order
ordered_df = df.sort("C_ACCTBAL", ascending=False)
ordered_df.show()

# Joining Customer and Orders tables on Customer Key
orders_df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS")
joined_df = df.join(orders_df, df["C_CUSTKEY"] == orders_df["O_CUSTKEY"])

# Calculating Running Total
window = Window.order_by(col("O_ORDERDATE"))
orders_df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS")
running_total_df = orders_df.with_column("running_total", sum(col("O_TOTALPRICE")).over(window))
running_total_df.show()

# Write a dataframe to a Snowflake database
running_total_df.write.mode("overwrite").save_as_table("orders_running_total")

# Define a Python function to calculate discount
def calculate_discount(price, discount):
    return price * (1 - discount / 100)

# Register the UDF with Snowflake
from snowflake.snowpark.types import FloatType
session.udf.register(
    func=calculate_discount,
    name="calculate_discount_udf",
    input_types=[FloatType(), FloatType()],
    return_type=FloatType()
)

# Use the UDF in a SQL query
result_df = session.sql(
    """
    SELECT O_CUSTKEY,
    calculate_discount_udf(O_TOTALPRICE, 10) AS discounted_price
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
    """
)
result_df.show()

# Stored Procedures: Copying data from one table to another
def copy_data(session: Session, source_table: str, target_table: str) -> str:
    source_df = session.table(source_table)
    source_df.write.save_as_table(target_table, mode="overwrite")
    return f"Data successfully copied from {source_table} to {target_table}"

session.sproc.register(
    func=copy_data,
    name="copy_data_sproc",
    input_types=[StringType(), StringType()],
    return_type=StringType(),
    packages=["snowflake-snowpark-python"]
)
#Calling the stored procedure
result = session.call("copy_data_sproc", "MY_DATABASE.MY_SCHEMA.SOURCE_TABLE", "MY_DATABASE.MY_SCHEMA.TARGET_TABLE")
print(result)

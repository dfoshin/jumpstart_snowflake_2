from snowflake.snowpark import Session
from snowflake.snowpark.types import FloatType, StringType
from sklearn.linear_model import LinearRegression
import pandas as pd
import pickle
import base64

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


# Step 1: Train a simple model
X = pd.DataFrame([1, 2, 3, 4, 5])
y = pd.DataFrame([2, 4, 5, 4, 5])
model = LinearRegression()
model.fit(X, y)

# Step 2: Serialize the model (Base64 encoding)
model_bytes = pickle.dumps(model)
model_base64 = base64.b64encode(model_bytes).decode("utf-8")  # Convert to Base64 string

# Step 3: Create a table to store the model (if not exists)
session.sql("CREATE TABLE IF NOT EXISTS model_storage (model VARIANT)").collect()

# Step 4: Store the model in Snowflake
session.sql(f"INSERT INTO model_storage SELECT PARSE_JSON('\"{model_base64}\"')").collect()

print("✅ Model successfully stored in Snowflake!")

# Step 5: Define a UDF for model prediction
def predict(model_base64, input_value):
    model_bytes = base64.b64decode(model_base64)
    model = pickle.loads(model_bytes)
    return model.predict([[input_value]])[0][0]

session.udf.register(
    func=predict,
    name="predict_linear",
    return_type=FloatType(),
    input_types=[StringType(), FloatType()],
    packages=["scikit-learn", "pandas", "numpy"],
    replace=True
)

print("✅ UDF 'predict_linear' registered successfully!")

# Step 6: Retrieve and deserialize the model from Snowflake
stored_model_base64 = session.sql("SELECT model::STRING FROM model_storage").collect()[0][0]
stored_model_bytes = base64.b64decode(stored_model_base64)
loaded_model = pickle.loads(stored_model_bytes)

# Step 7: Test the retrieved model
test_input = 6
predicted_output = loaded_model.predict([[test_input]])[0][0]
print(f"✅ Prediction for input {test_input}: {predicted_output}")

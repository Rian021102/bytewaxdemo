import pandas as pd
from datetime import timedelta, datetime, timezone
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import CSVInput
from bytewax.connectors.stdio import StdOutput
from bytewax.window import SystemClockConfig, TumblingWindow

# Function to print csv file
def print_csv(row):
    df = pd.DataFrame([row])
    print(df)




clock_config = SystemClockConfig()
window_config = TumblingWindow(
    length=timedelta(seconds=2), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
)

#perform data flow for each row
# Dataflow
flow = Dataflow()
flow.input("inp", CSVInput("/Users/rianrachmanto/pypro/project/Smoker-Detection-Bio-Signs/data/raw/test_smoking.csv"))
# Map to both print_csv and csv_to_dataframe functions
flow.map(print_csv)
flow.output("out", StdOutput())


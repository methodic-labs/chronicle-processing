import pandas as pd
import os
import yaml

# to write test after careful validations

directory = "/Users/kimengie/Dev/chronicle-analysis/pymethodic/test/"
encountered_file =  os.path.join(
    directory, 
    'resources/output/summary_daily.csv'
)

expected_file = os.path.join(
    directory, 
    'resources/constants/constants_summary.yaml'
)

encountered_output = pd.read_csv(encountered_file).to_dict()

with open(expected_file, 'w') as outfile:
    yaml.dump(encountered_output, outfile)


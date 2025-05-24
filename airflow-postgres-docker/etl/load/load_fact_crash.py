from ..transform.transform_all import perform_transformation_crash

import pandas as pd

def load_fact_crash(filepath_in):
    crash_df = pd.read_csv(filepath_in)
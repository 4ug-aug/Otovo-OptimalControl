from turtle import pd
import pandas as pd
from pprint import pprint

data = pd.read_csv('year_coverage_consumption_100.csv')
data2 = pd.read_csv('year_coverage_production_100.csv')

year_coverage_2_cons = data[data['time_between_first_last'] >= 2]['meter_id']
year_coverage_2_prod = data2[data2['time_between_first_last'] >= 2]['meter_id']

# Print all meter-ids with time_between_first_last >= 2
print(len(year_coverage_2_cons))
print(len(year_coverage_2_prod))

# print which meters are in consumption but not in production
difference_cons = set(year_coverage_2_cons).difference(set(year_coverage_2_prod))
difference_prod = set(year_coverage_2_prod).difference(set(year_coverage_2_cons))

print("Meter-ids with 2 year coverage in consumption but not in production:")
pprint(difference_cons)
print()
print("Meter-ids with 2 year coverage in production but not in consumption:")
pprint(difference_prod)
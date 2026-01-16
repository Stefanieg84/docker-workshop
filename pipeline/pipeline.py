import sys
import pandas as pd


print('arguments', sys.argv, file=sys.stderr)

month = int(sys.argv[1])

df = pd.DataFrame({"day": [1, 2, 3], "num_passengers": [4, 5, 6]})
df['month'] = month
print(df.head(), file=sys.stderr)

df.to_parquet(f"output_{month}.parquet", index=False)

print(f'Hello from pipeline/pipeline.py, Month={month}', file=sys.stderr)
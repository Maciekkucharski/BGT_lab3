import pandas as pd
import dask.dataframe as dd

files = []
for i in range(3):
    files.append(f"{i}_parquet_file.gzip")
#one file is broken
files.remove("1_parquet_file.gzip")
df = dd.read_parquet(files)
print(df)
repo_names = df["repo_name"].compute()
distinct_repo = dict()
print(repo_names)
for repo_list in repo_names:
    for repo in repo_list:
        if repo in distinct_repo:
            distinct_repo[repo] += 1
        else:
            distinct_repo[repo] = 1
distinct_repo = dict(sorted(distinct_repo.items(), key=lambda x: x[1]))
for key, value in distinct_repo.items():
    distinct_repo[key] = {"commits": value}
    distinct_repo[key]['distinct_authors'] = set()
df2 = df[['repo_name','author']].compute()
print(distinct_repo)
test_dict = {}
for commit_index, row in df2.iterrows():
    test_dict[row['repo_name']] = 

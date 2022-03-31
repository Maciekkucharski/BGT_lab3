import pandas as pd
import dask.dataframe as dd

files = []
for i in range(1):
    files.append(f"{i}_parquet_file.gzip")
# one file is broken
# files.remove("1_parquet_file.gzip")
df = dd.read_parquet(files)

goal_table = df[['repo_name', 'author']].compute()
repo_names = goal_table['repo_name']

distinct_repo = dict()
for repo_list in repo_names:
    for repo in repo_list:
        if repo in distinct_repo:
            distinct_repo[repo]['commits'] += 1
        else:
            distinct_repo[repo] = dict()
            distinct_repo[repo]['commits'] = 1
            distinct_repo[repo]['distinct_authors'] = set()

distinct_repo = dict(sorted(distinct_repo.items(), key=lambda x: x[1]['commits']))

for commit_index, row in goal_table.iterrows():
    for repo in row['repo_name']:
        distinct_repo[repo]['distinct_authors'].add(row['author']['email'])

for key, value in distinct_repo.items():
    distinct_repo[key]['distinct_authors'] = len(distinct_repo[key]['distinct_authors'])

print(distinct_repo)


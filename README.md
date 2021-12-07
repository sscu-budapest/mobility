# ANET Mobility project

## Reports so far

## To play around with the data

### 1. fork and clone the repo

```
git clone https://github.com/sscu-budapest/mobility
```
(preferably fork it first)

### 2. install deps 

```
pip install -r requirements.txt
```

### 3. get the sample data


if you are using the anet server: 
```
dvc pull
```

otherwise, set up the anet server to be `anetcloud` in ssh config and then
```
dvc pull --remote anetcloud-ssh
```


### 4. load some samples and look around

```python
from src.data_dumps import ParsedCols
from src.load_samples import covid_tuesday

def total_range(s):
    return s.max() - s.min()

samp_df = covid_tuesday.get_full_df()

samp_df.groupby(ParsedCols.user).agg(
    {
        ParsedCols.lon: ["std", total_range],
        ParsedCols.lat: ["std", total_range],
        ParsedCols.dtime: ["min", "max", "count"],
    }
).agg(["mean", "median"]).T
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>mean</th>
      <th>median</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2" valign="top">lon</th>
      <th>std</th>
      <td>0.033919</td>
      <td>0.001588</td>
    </tr>
    <tr>
      <th>total_range</th>
      <td>0.080638</td>
      <td>0.000471</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">lat</th>
      <th>std</th>
      <td>0.01889</td>
      <td>0.001067</td>
    </tr>
    <tr>
      <th>total_range</th>
      <td>0.045475</td>
      <td>0.000336</td>
    </tr>
    <tr>
      <th rowspan="3" valign="top">dtime</th>
      <th>min</th>
      <td>2020-11-03 07:38:23.900066048</td>
      <td>2020-11-03 06:33:42</td>
    </tr>
    <tr>
      <th>max</th>
      <td>2020-11-03 18:43:00.657093888</td>
      <td>2020-11-03 21:08:10</td>
    </tr>
    <tr>
      <th>count</th>
      <td>87.307432</td>
      <td>21.0</td>
    </tr>
  </tbody>
</table>

### + if you want to run something that can run on the full data set, I suggest using dask

```python
from src.data_dumps import ParsedCols
from src.load_samples import covid_tuesday
import matplotlib.pyplot as plt


samp_ddf = covid_tuesday.get_full_ddf()

ddf_aggs = (
    samp_ddf.assign(hour=lambda df: df[ParsedCols.dtime].dt.hour)
    .groupby("hour")
    .agg({ParsedCols.lon: ["std"], ParsedCols.lat: "std", "dtime": "count"})
    .compute()
)

fig, ax1 = plt.subplots()

ddf_aggs.iloc[:, :2].plot(figsize=(14, 7), ax=ax1, xlabel="hour in the day").legend(
    loc="center left"
)
ddf_aggs.loc[:, "dtime"].plot(figsize=(14, 7), ax=ax1.twinx(), color="green").legend(
    loc="center right"
)
```
![fig1](report-eg.png)

### load a full week of data


```python
from src.create_samples import covid_sample, non_covid_sample

# this is about 3GB of memory, use get_full_ddf for lazy dask dataframe
cov_df = covid_sample.get_full_df()
```

## TODO

- "reliable user" counts
  - number of pings
  - "do we know where they live"
  - every month at least once a week
  - 30 / day (?)
  - 3 in teh morning, 3 in teh evening

- dump by month
- dump by user

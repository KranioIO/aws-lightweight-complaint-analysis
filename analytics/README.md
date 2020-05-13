### Requirements

* node
* npm
* python
* pip
* awscli
* docker
* serverless@1.51.0

### Setup & Deploy

```
npm install
sls deploy -s dev --aws-profile kranio
```

### Code guide

File to be changed:

`functions/apply_transformation.py` > `apply_transformation` method

#### Variables to be changed:

* source_prefix - Prefix for a data to be transform
* target_prefix - Prefix for save the transformed data

_The variables without initial/final slash and filename, example for a valid prefix: `raw/complaints`_

#### Fixed prefix:

* __raw/__ - Raw data
* __enriched/__ - Transformed Data


#### Raw Data:

`raw/ratings/ratings.csv`

`raw/complaints/complaints.csv`

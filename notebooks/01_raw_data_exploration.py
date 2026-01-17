#!/usr/bin/env python
# coding: utf-8

# # Airbnb London – Raw Data Exploration & Monthly Processing
# 
# This notebook explores raw Airbnb datasets and prepares month-specific
# processed files (November and December 2024) to be used in an AWS-based
# data lake architecture.

# ## Dataset Access & Project Setup

# In[1]:


from pathlib import Path

# Base directories
PROJECTS_DIR = Path.home() / "projects"
DATA_DIR = PROJECTS_DIR / "data"
RAW_DIR = DATA_DIR / "raw"

RAW_DIR


# In[2]:


list(RAW_DIR.iterdir())


# In[3]:


listings_path = RAW_DIR / "listings.csv.gz"
reviews_path = RAW_DIR / "reviews.csv.gz"
calendar_path = RAW_DIR / "calendar.csv.gz"

listings_path, reviews_path, calendar_path


# ## Raw Data Exploration

# In[4]:


import pandas as pd

listings_sample = pd.read_csv(
    listings_path,
    compression="gzip",
    nrows=2000
)

listings_sample.head()


# In[5]:


listings_sample.shape


# In[6]:


listings_sample.columns.tolist()


# ## Reviews Dataset – Time-Based Filtering

# In[7]:


reviews_sample = pd.read_csv(
    reviews_path,
    compression="gzip",
    nrows=2000,
    parse_dates=["date"]
)

reviews_sample.head()


# In[8]:


reviews_sample["date"].min(), reviews_sample["date"].max()


# In[9]:


reviews_sample["date"].dt.to_period("M").value_counts().sort_index().tail(24)


# ## Monthly Dataset Processing (Raw → Processed)

# In[10]:


import shutil

PROCESSED_DIR = (Path.home() / "projects" / "data" / "processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

out_listings_nov = PROCESSED_DIR / "2024-11-listings.csv.gz"
out_listings_dec = PROCESSED_DIR / "2024-12-listings.csv.gz"

# Listings data is a snapshot and not time-partitioned,
# so we reuse the same dataset for both months.
shutil.copy2(listings_path, out_listings_nov)
shutil.copy2(listings_path, out_listings_dec)

out_listings_nov, out_listings_dec


# In[11]:


out_reviews_nov = PROCESSED_DIR / "2024-11-reviews.csv.gz"

# If file exists from earlier runs, remove it for a clean write
if out_reviews_nov.exists():
    out_reviews_nov.unlink()

first_write = True

for chunk in pd.read_csv(
    reviews_path,
    compression="gzip",
    parse_dates=["date"],
    chunksize=500_000,
):
    nov = chunk[(chunk["date"] >= "2024-11-01") & (chunk["date"] < "2024-12-01")]
    if not nov.empty:
        nov.to_csv(
            out_reviews_nov,
            mode="a",
            index=False,
            header=first_write,
            compression="gzip",
        )
        first_write = False

out_reviews_nov


# In[12]:


out_reviews_dec = PROCESSED_DIR / "2024-12-reviews.csv.gz"

# If file exists from earlier runs, remove it for a clean write
if out_reviews_dec.exists():
    out_reviews_dec.unlink()

first_write = True

for chunk in pd.read_csv(
    reviews_path,
    compression="gzip",
    parse_dates=["date"],
    chunksize=500_000,
):
    dec = chunk[(chunk["date"] >= "2024-12-01") & (chunk["date"] < "2025-01-01")]
    if not dec.empty:
        dec.to_csv(
            out_reviews_dec,
            mode="a",
            index=False,
            header=first_write,
            compression="gzip",
        )
        first_write = False

out_reviews_dec


# In[13]:


sorted(PROCESSED_DIR.iterdir())


# ### Processed Dataset Summary
# 
# The following processed datasets were created from the raw Airbnb data:
# 
# - Listings snapshots for November and December 2024 (renamed copies)
# - Reviews filtered by month for November and December 2024
# 
# These datasets will be used in subsequent steps to build and query an AWS-based data lake.

# In[ ]:





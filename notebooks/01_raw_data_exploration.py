#!/usr/bin/env python
# coding: utf-8

# ## 1. Dataset Access & Project Setup

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


# ## 2. Raw Data Exploration

# In[6]:


import pandas as pd

listings_sample = pd.read_csv(
    listings_path,
    compression="gzip",
    nrows=2000
)

listings_sample.head()


# In[8]:


listings_sample.shape


# In[9]:


listings_sample.columns.tolist()


# In[ ]:





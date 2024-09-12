# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: work
#     language: python
#     name: python3
# ---

# %%
import pandas

# %%
data = pandas.read_csv(
    "../../../_products/rates-processing/departmental-csmr-merged-and-renamed.csv",
    )

# %%
data

# %%

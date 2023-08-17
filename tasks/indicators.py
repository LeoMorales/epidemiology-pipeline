import pandas


def get_causes_incidence(upstream, product):
    df_all_deceases = pandas.read_parquet(
        str(upstream["aggr-deceases-by-sex-by-year-arg"])
    )
    df_causes_subset_deceases = pandas.read_parquet(
        str(upstream["aggr-cause-specific-deceases-by-sex-by-year-arg"])
    )

    # 3. combine
    df = pandas.merge(df_all_deceases, df_causes_subset_deceases, on="year", how="left")

    incidence_df = df[["year"]].copy()

    incidence_df["csmr"] = (df["cause_specific_deceases"] / df["deceases"]) * 100_000

    incidence_df["csmr_male"] = (
        df["cause_specific_deceases_male"] / df["deceases"]
    ) * 100_000

    incidence_df["csmr_female"] = (
        df["cause_specific_deceases_female"] / df["deceases"]
    ) * 100_000

    incidence_df["csmr_male_local"] = (
        df["cause_specific_deceases_male"] / df["deceases_male"]
    ) * 100_000

    incidence_df["csmr_female_local"] = (
        df["cause_specific_deceases_female"] / df["deceases_female"]
    ) * 100_000

    incidence_df.to_parquet(str(product))

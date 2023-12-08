import pandas
from surnames_package import utils


def cleaningAnnualPopulationProjectionsForSexByAgeGroup(product):
    # utils functions:
    def splitDatasetGroups(df):
        """Analiza el Cuadro 2. Población por sexo y grupos quinquenales de edad. Total del país. Años 2010-2040.
        Cada hoja de proyecciones tiene una tabla (*) con la información de (máximo) seis años. Por cada año, se presentan tres columnas: Ambos sexo, Varones y Mujeres y para rangos etarios de cinco años.
        El archivo se completa con con tablas como la anterior, hasta completar el período de 2010 al 2040.

        Se extraen todas las tablas (*) que contienen la información de máximo seis años.

        Args:
            df (pandas Dataframe): Dataframe

        Returns:
            list: Lista con dataframes. Cada dataframe tiene las proyecciones de población (total, varones, mujeres) para al menos 6 años.
        """
        df = df.dropna(how="all")
        df = df.dropna(axis="columns", how="all")

        renaming = {df.columns[0]: "description", df.columns[-1]: "ultima"}

        df = df.rename(columns=renaming)

        df = df.reset_index(drop=True)

        # Initialize a list to store DataFrames for each table
        table_dataframes = []

        # Initialize variables to keep track of the table boundaries
        start_index = 0  # Start of the current table

        # Iterate through the DataFrame to find and split tables
        for index, row in df.iterrows():
            if row["ultima"] == "(continúa)":
                # Found the end of a table, extract the table data
                table_df = df.iloc[start_index:index].copy()
                table_dataframes.append(table_df)
                start_index = index + 1  # Update the start index for the next table

        # Add the last table (after the last "next" value)
        table_df = df.iloc[start_index:].copy()
        table_dataframes.append(table_df)

        return table_dataframes

    def standarizeDfColumns(df):
        """Renombra las columnas según los valores de las ya existentes.
        Edad, año, ambos sexos, varones, mujeres --> age_group, total_[año], male_[año], female_[año]

        Args:
            df (pandas Dataframe): Dataframe con nombre de columna en dos renglones.

        Returns:
            pandas Dataframe: Toma una tabla con los nombres de columnas a la misma tabla con un renglon.
        """
        slice_df = df.dropna(axis="columns", how="all").reset_index(drop=True).copy()
        years = (
            slice_df[slice_df["description"] == "Edad"]
            .loc[0]
            .dropna()
            .loc[lambda value: value != "Edad"]
            .values
        )

        categories = ["total", "male", "female"]  # Your list of categories

        # Use list comprehension to generate the combined values
        combined_values = [
            f"{category}_{year}" for year in years for category in categories
        ]

        # 'combined_values' now contains the desired values
        # print(combined_values)
        new_columns = ["age_group"] + combined_values

        slice_df = slice_df[3:]
        slice_df.columns = new_columns

        return slice_df.dropna().reset_index(drop=True)

    def extractYearlyData(df):
        """Transforma dataset con información con años uno al lado del otro en un dataset uno abajo del otro

        Args:
            df (pandas Dataframe): Información anual en forma horizontal.

        Returns:
            pandas Dataframe: Información en forma vertical.
        """
        # Create separate DataFrames for each year
        years = sorted(
            list(
                set(
                    map(
                        lambda e: e.split("_")[-1],
                        filter(lambda e: e != "age_group", df.columns.values),
                    )
                )
            )
        )

        yearly_data = {}

        for year in years:
            total_col = f"total_{year}"
            female_col = f"male_{year}"
            male_col = f"female_{year}"

            year_df = df[["age_group", total_col, female_col, male_col]].copy()

            year_df = year_df.rename(
                columns={
                    f"total_{year}": "total",
                    f"male_{year}": "male",
                    f"female_{year}": "female",
                }
            )

            year_df["year"] = year

            yearly_data[year] = year_df

        # se preserva yearly_data como diccionario por futuro mantenimiento.

        return pandas.concat(yearly_data.values())

    data_in_excel_path = "/home/lmorales/work/pipelines/epidemiology_pipeline/public-data/c2_proyecciones_prov_2010_2040 (1).xls"
    xl = pandas.ExcelFile(data_in_excel_path)

    sheet_names = xl.sheet_names[
        2:
    ]  # all sheet names menos 'GraphData', '01-TOTAL DEL PAÍS',

    provincial_data = {}

    for province_sheet_name in sheet_names:
        df = pandas.read_excel(
            data_in_excel_path,
            sheet_name=province_sheet_name,
        )

        table_dataframes = splitDatasetGroups(df)

        population_df = pandas.concat(
            [
                extractYearlyData(standarizeDfColumns(table_dataframe_i))
                for table_dataframe_i in table_dataframes
            ]
        ).reset_index(drop=True)

        population_df["province"] = utils.PROVINCE_NAME_BY_ID.get(
            province_sheet_name.split("-")[0], "NOT-FOUND"
        )

        provincial_data[province_sheet_name] = population_df

    output_df = pandas.concat(provincial_data.values()).reset_index(drop=True)
    output_df.to_parquet(str(product))

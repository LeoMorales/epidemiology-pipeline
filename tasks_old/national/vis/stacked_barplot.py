import pandas
from epidemiology_package import cleanning
from epidemiology_package import vis
from epidemiology_package import utils


def draw_stacked_bars_with_amounts_by_code_category(
    upstream, product, colorsForEachGroup, causeName, criterion, applyDataNormalization
):
    """
    Crear un gráfico de barras apiladas.
    Una pila por año.
    Con los códigos indicados en el agrupamiento.

    Args:

        codeColors (list): Contiene el color asignado a cada clave en codesGrouping.
            TODO: podría ser un dict

        causeName (str): Nombre representativo del conjunto de causas (códigos).
            Puede ser por ejemplo EPOF, Alz, etc.
    """
    DECEASE_CODE_COL = "codigo_defuncion"
    AGGR_DECEASE_CODE_COL = "codigo_defuncion_aggr"
    PERIOD_COL = "year"
    # leer
    df_causes = pandas.read_parquet(
        str(upstream["filter-cause-specific-deceases-1991-2017"])
    )

    # obtener el agrupamiento para los códigos utilizados (agrupamiento=dict)
    # el agrupamiento devuelve un diccionario con categorias fijas
    _codesGrouping = utils.get_codes_categorization(
        df_causes[DECEASE_CODE_COL].unique()
    )

    # reescribir (para agrupar)
    df_causes[AGGR_DECEASE_CODE_COL] = cleanning.rewrite_codes_according_to_grouping(
        values=df_causes[DECEASE_CODE_COL],
        codesGrouping=_codesGrouping,
        defaultValue="others",
    )

    # diferenciar la perspectiva que estamos utilizando
    filterSelector = {
        "all": [1, 2],
        "male": [1],
        "female": [2],
    }
    correspondingSexCodes = filterSelector[criterion]
    df_causes = df_causes[df_causes["sexo"].isin(correspondingSexCodes)]

    # contar
    causes_counts_df = (
        df_causes[["provincia_id", PERIOD_COL, AGGR_DECEASE_CODE_COL]]
        .groupby([PERIOD_COL, AGGR_DECEASE_CODE_COL])
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "count"})
    )

    # crear un dataframe con ceros para todos los años y codigos
    unique_years = sorted(df_causes[PERIOD_COL].unique())
    unique_code_group = sorted(df_causes[AGGR_DECEASE_CODE_COL].unique())
    data = []
    for year in unique_years:
        for codeGroup in unique_code_group:
            data.append([year, codeGroup, 0])
    empty_df = pandas.DataFrame(
        data=data, columns=[PERIOD_COL, AGGR_DECEASE_CODE_COL, "count"]
    )

    # rellenar el dataset vacio
    counts_df = (
        empty_df.merge(
            causes_counts_df, on=[PERIOD_COL, AGGR_DECEASE_CODE_COL], how="left"
        )
        .drop(columns="count_x")
        .rename(columns={"count_y": "count"})
        .fillna(0)
    )
    counts_df["count"] = counts_df["count"].astype(int)

    #    year codigo_defuncion_aggr  count
    # 0  1997                     I     68
    # 1  1997                    II   6456
    # 2  1997                   III     38

    # armar un dataset con los años en el indice y los codigos en las columnas:
    plot_data = counts_df.pivot(
        index=PERIOD_COL, columns=AGGR_DECEASE_CODE_COL, values="count"
    )
    plot_data.index.name = None
    plot_data.columns.name = None

    #        I    II  III  IV ...  XVII  XVIII
    # 1997  68  6456   38  90 ...   743     16
    # 1998  79  6550   47  92 ...   774     10
    # 1999  71  6534   52  78 ...   758      6

    # We create a structure with tuples that allows us to discard the categories that do not have codes
    # in this problem and keep the colors when toggling "all, male, female"
    presentCategories = counts_df["codigo_defuncion_aggr"].unique()
    amountForEachCategory = list(map(lambda l: len(l), _codesGrouping.values()))
    categoryNames = _codesGrouping.keys()
    colors = list(map(lambda k: colorsForEachGroup[k], _codesGrouping.keys()))
    categoryColors = list(zip(categoryNames, colors, amountForEachCategory))
    _codeColors = [
        color
        for category, color, amount in categoryColors
        if ((category in presentCategories) and (amount > 0))
    ]

    # TODO obtener los máximos por año para mantener el eje x independientemente del criterio recibido.
    # dibujar!
    applyDataNormalization = applyDataNormalization == "normalized"
    sorted_plot_data = plot_data.sort_index(ascending=False)
    vis.draw_stacked_plot(
        sorted_plot_data,
        only_save=True,
        output_file=str(product),
        title=f"{causeName} deceases by codes ({criterion})",
        colors=_codeColors,
        legend_n_rows=len(_codesGrouping) // 4 if len(_codesGrouping) > 1 else 1,
        legend_font_size=14,
        applyDataNormalization=applyDataNormalization,
    )

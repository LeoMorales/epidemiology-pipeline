from epidemiology_package.report import Report
from epidemiology_package import utils
import pandas


def create_report(upstream, product, causeName, causesCodes, ageCategoryCauses):
    causesCodes_used = ", ".join([str(code) for code in causesCodes])
    ageCategoryCauses_used = ", ".join(ageCategoryCauses)
    _codesGrouping = utils.get_codes_categorization(pandas.Series(causesCodes))
    codesGrouping_used = "\n".join(
        [f'<li>"{k}" -> {v}</li>' for k, v in _codesGrouping.items()]
    )
    codesGrouping_used = f'<ul>"{codesGrouping_used}</ul>'

    report = Report(
        title=causeName,
        subtitle="Epidemiology report",
        experiment_params=[
            {
                "title": "Códigos CIE",
                "desc": f"Listado de códigos referidos a {causeName} ({len(causesCodes)})",
                "value": causesCodes_used,
            },
            {
                "title": "Categorias",
                "desc": "Grupos etarios definidos",
                "value": ageCategoryCauses_used,
            },
            {
                "title": "Agrupamientos",
                "desc": "Los códigos de las causas se agruparon según las siguientes características",
                "value": codesGrouping_used,
            },
        ],
    )

    # add section:
    report.add_section(
        title=f"Cantidad de fallecidos por la causa {causeName}",
        figure=str(upstream["draw-stacked-bars-with-amounts-by-code-category-all"]),
    )
    report.add_section(
        title=f"Cantidad de fallecidos por la causa {causeName} (varones)",
        figure=str(upstream["draw-stacked-bars-with-amounts-by-code-category-male"]),
    )
    report.add_section(
        title=f"Cantidad de fallecidos por la causa {causeName} (mujeres)",
        figure=str(upstream["draw-stacked-bars-with-amounts-by-code-category-female"]),
    )
    desc = """
        <p>I		- A00-B99 - Ciertas enfermedades infecciosas y parasitarias </p>
        <p>II		- C00-D48 - Neoplasias </p>
        <p>III		- D50-D89 - Enfermedades de la sangre y de los órganos hematopoyéticos y otros trastornos que afectan el mecanismo de la inmunidad </p>
        <p>IV		- E00-E90 - Enfermedades endocrinas, nutricionales y metabólicas </p>
        <p>V		- F00-F99 - Trastornos mentales y del comportamiento </p>
        <p>VI		- G00-G99 - Enfermedades del sistema nervioso </p>
        <p>VII		- H00-H59 - Enfermedades del ojo y sus anexos </p>
        <p>VIII	    - H60-H95 - Enfermedades del oído y de la apófisis mastoides </p>
        <p>IX		- I00-I99 - Enfermedades del aparato circulatorio </p>
        <p>X		- J00-J99 - Enfermedades del aparato respiratorio </p>
        <p>XI		- K00-K93 - Enfermedades del aparato digestivo </p>
        <p>XII		- L00-L99 - Enfermedades de la piel y el tejido subcutáneo </p>
        <p>XIII	    - M00-M99 - Enfermedades del sistema osteomuscular y del tejido conectivo </p>
        <p>XIV		- N00-N99 - Enfermedades del aparato genitourinario </p>
        <p>XV		- O00-O99 - Embarazo, parto y puerperio </p>
        <p>XVI		- P00-P96 - Ciertas afecciones originadas en el periodo perinatal </p>
        <p>XVII	    - Q00-Q99 - Malformaciones congénitas, deformidades y anomalías cromosómicas </p>
        <p>XVIII	- R00-R99 - Síntomas, signos y hallazgos anormales clínicos y de laboratorio, no clasificados en otra parte </p>
        <p>XIX		- S00-T98 - Traumatismos, envenenamientos y algunas otras consecuencias de causa externa </p>
        <p>XX		- V01-Y98 - Causas externas de morbilidad y de mortalidad </p>
        <p>XXI		- Z00-Z99 - Factores que influyen en el estado de salud y contacto con los servicios de salud </p>
        <p>XXII	    - U00-U99 - Códigos para situaciones especiales </p>
    """
    report.add_section(
        title=f"Referencia:",
        text=desc,
    )
    report.add_section(
        title=f"Cantidad de fallecidos en Argentina",
        text="Fallecimientos de varones y mujeres por grupo etario",
        figure=str(
            upstream["draw-barchart-for-all-deceases-by-sex-and-grouped-by-age"]
        ),
    )

    report.add_section(
        title=f"Cantidad de fallecidos por la causa {causeName}",
        text="Fallecimientos de varones y mujeres por grupo etario",
        figure=str(
            upstream[
                "draw-barchart-for-deceases-from-specific-causes-by-sex-and-grouped-by-age"
            ]
        ),
    )

    report.add_section(
        title=f"Fallecimientos totales y específicos en Argentina",
        text="Fallecimientos de varones y mujeres por grupo etario",
        figure=str(upstream["draw-barchart-for-deceases-by-sex-and-grouped-by-age"]),
    )

    report.add_section(
        title=f"Tendencia de fallecimientos",
        text="Cantidades de fallecidos por año y sexo",
        figure=str(upstream["draw-deceases-lineplots-1991-2017"]),
    )

    report.add_section(
        title=f"Incidencia",
        text=f"Fallecimientos a causa de {causeName} por cada 100,000 fallecimientos",
        figure=str(upstream["draw-incidence-lineplots-1991-2017"]),
    )

    report.add_section(
        title="Tasas anuales por grupo etario",
        text=f"Tasa: Fallecimientos a causa de {causeName} por cada 1,000 fallecimientos",
        figure=str(upstream["draw-age-grouping-rates-lineplot"]),
    )

    report.add_section(
        title="Tasa de mortallidad por causas espacíficas (Cause-specifict mortality rate, CSMR).",
        text=f"Tasa: Fallecimientos a causa de {causeName} por cada 1,000 fallecimientos en cada provincia",
        figure=str(upstream["draw-csmr-heatmap-provincial"]),
    )

    report.add_section(
        title=f"CSMR para la region NOA por grupo etário.",
        figure=str(upstream["draw-csmr-heatmap-provincial-of-region-NOA"]),
    )
    report.add_section(
        title=f"CSMR para la region NEA por grupo etário.",
        figure=str(upstream["draw-csmr-heatmap-provincial-of-region-NEA"]),
    )
    report.add_section(
        title=f"CSMR para la region Centro por grupo etário.",
        figure=str(upstream["draw-csmr-heatmap-provincial-of-region-Centro"]),
    )
    report.add_section(
        title=f"CSMR para la region Cuyo por grupo etário.",
        figure=str(upstream["draw-csmr-heatmap-provincial-of-region-Cuyo"]),
    )
    report.add_section(
        title=f"CSMR para la region Patagonia por grupo etário.",
        figure=str(upstream["draw-csmr-heatmap-provincial-of-region-Patagonia"]),
    )

    # save:
    report.build(str(product))

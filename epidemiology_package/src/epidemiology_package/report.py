# -*- coding: utf-8 -*-
import pdfkit
import PIL
import datetime
import os


# CONSTANTS:
PDF_EXPORT_OPTIONS = {
    'page-size': 'Letter',
    'margin-top': '0.75in',
    'margin-right': '0.75in',
    'margin-bottom': '0.75in',
    'margin-left': '0.75in',
    'encoding': "UTF-8",
    'no-outline': None,
    'footer-left': "Last update here",
    'footer-line':'',
    'footer-font-size':'7',
    'footer-right': '[page] of [topage]'       
}

BR = '<div style="display:block; clear:both; page-break-after:always;"></div>'

IMGS_BASEWITH = 1000


class Report:
    ''' Usage:
    
    # instantiate report obj
    report = Report(
        "Title", "Subtitle regional analysis",
        [{
            'title': 'Tasas',
            'desc': 'Para este reporte las tasas fueron calculadas sobre:',
            'value': '1000',
        }]
    )

    # add section:
    report.add_section(
        '<h2 align="center">Provincial view</h2>')
    
    # add section with page break:
    report.add_section(
        '<h2 align="center">Country view</h2>',
        finish_with_page_break=True
    )

    # save:
    report.build(str(product))  
    '''
    
    def __init__(self, title: str, subtitle: str, experiment_params: list):
        ''' 
        experiment_params = [
            {
                param_title: 'Tasas',
                param_desc: 'Para este reporte las tasas fueron calculadas sobre:',
                param_value: '1000',
            },
        ]
        '''
        self.title = title
        self.subtitle = subtitle
        self.experiment_params = experiment_params
        
        self.tmp_files = []
        self.content = ''
        self.params = self.get_html_params()

    def get_html_params(self):
        build_params = '<h3>Parámetros:</h3>'
        if len(self.experiment_params) == 0:
            build_params += '<p>---</p>'
        
        for param in self.experiment_params:
            build_params += f'''
                <p>{param["title"]}:</p>
                <ul><li>{param["desc"]}: {param["value"]}</li></ul>
            '''

        return build_params

    def add_section(
            self, title: str,
            text: str = None,
            figure: str = None,
            caption:str = None,
            finish_with_page_break = True,
            figure_basewidth: float = None
        ) -> None:
        '''Agrega una sección al reporte.
        Puede contener una figura: parámetro `figure`.
        
        Args:
            text (str): Título/párrafo de la sección en formato html.
            figure (str): Ruta a una imagen.
            caption (str): Texto debajo de la imagen de la sección.
            finish_with_page_break (bool): Agregar o no un espacio luego de la sección.
            
        Returns:
            None
        '''
        # add section (title, fig, caption):
        
        # > title
        report_content = f'<h2>{title}</h2>'
        if text:
            report_content += f'<p">{text}</p>'
        
        # > figure
        if figure:
            RESIZED_IMAGE_DIR, w, h = self.__resize_image(
                figure,
                basewidth=(figure_basewidth if figure_basewidth else IMGS_BASEWITH)
            )
            report_content += self.__get_fig_template(RESIZED_IMAGE_DIR, w, h)
            #report_content += BR
            self.tmp_files.append(f"{os.getcwd()}/{RESIZED_IMAGE_DIR}")
        
        # > caption
        if caption:
            report_content += caption
        
        if finish_with_page_break:
            report_content += BR
        
        self.content += f"<div>{report_content}</div>"

    def __get_report_text(self):
        return f'''
            <!DOCTYPE html>
                <html>
                    <head>
                        <meta charset='utf-8'>
                        <title>{self.title}</title>
                        <style>
                            h1, h2, h3 {{
                                font-family: Arial;
                            }}
                        </style>                       
                    </head>
                    <body>
                        <div style="margin-top:150px;">
                            <h1 align="center">{self.title}</h1>
                            <h2 align="center">{self.subtitle}</h2>
                            <br>
                            <br>
                            {self.params}
                        </div>
                        <div style="display:block; clear:both; page-break-after:always;"></div>
                        <div>
                            {self.content}
                        </div>
                    </body>
                </html>
            '''
    
    def build(self, destination: str, page_size: str = 'Letter'):
        '''Genera el PDF con el contenido del reporte'''
        # destination = str(product)
        
        # set current time
        PDF_EXPORT_OPTIONS['footer-left'] = f"Date of the last update: {str(datetime.datetime.now())}"
        PDF_EXPORT_OPTIONS['page-size'] = page_size,
        # Save HTML string to file
        HTML_REPORT_DIR = "tmp_report_years.html"

        with open(HTML_REPORT_DIR, "w") as r:
            r.write(self.__get_report_text())

        #Use pdfkit to create the pdf report from the 
        pdfkit.from_file(
            HTML_REPORT_DIR,
            destination,
            options=PDF_EXPORT_OPTIONS
        ) 

        # remove the tmp data
        os.remove(HTML_REPORT_DIR)
        
        for tmp_file in self.tmp_files:
            print(f"removing {tmp_file}...")
            os.remove(tmp_file)

    def __resize_image(self, image_path, basewidth):
        input_image = PIL.Image.open(image_path)
        img_width, img_height = input_image.size

        wpercent = (basewidth / float(img_width))
        hsize = int((float(img_height) * float(wpercent)))
        img = input_image.resize((basewidth, hsize), PIL.Image.ANTIALIAS)
        base_name = image_path.split('/')[-1].rstrip('.png')
        output_image_path = f'{base_name}_{datetime.datetime.now().isoformat()}.png'
        img.save(output_image_path)
        
        return output_image_path, basewidth, hsize

    def __get_fig_template(self, fig_src: str, width: int, height: int) -> str:
        return f'''
            <figure align="center">
                <img align="center" src="{fig_src}" width="{width}" height="{height}">
            </figure>
        '''  



import pandas as pd; pd.set_option('display.max_columns', 500); pd.set_option('display.max_rows', 100)
import numpy as np
from bs4 import BeautifulSoup as bs
import requests
import datetime
import os
import re

def main():
    # Generate unique filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo_datos = f'datos_scrape_argenprop_tres_de_febrero{timestamp}.csv'
    
    print(f"Creando nuevo archivo: {nombre_archivo_datos}")

    # Scrape de nuevas publicaciones
    # Tope de páginas a scrapear
    pages = 50

    data = []
    seen_ids = set()
    consecutive_duplicates = 0
    max_consecutive_duplicates = 5  # Detener después de 5 duplicados consecutivos
    
    for i in range(1, pages + 1):
        print(f'Scrapeando la página {i}...')
        
        try:
            response = requests.get("https://www.argenprop.com/departamento-alquiler-localidad-tres-de-febrero-orden-masnuevos-pagina-" + str(i), headers={'User-Agent': 'Chrome'})
            response.raise_for_status()  # Raise exception for bad status codes
            soup = bs(response.text, 'html5lib')
            listings = soup.find_all('div', class_=lambda x:x=='listing__item' if x else False)
        except requests.RequestException as e:
            print(f"Error al acceder a la página {i}: {e}")
            continue
            
        # Por cada publicación encontrada, extraer los datos y appendear a la lista "data"
        page_has_new_data = False
        for listing in listings:
            
            id_ = listing.find('a')

            # Chequear que sea una publicación válida, si no lo es, pasar a la siguiente
            if id_:
                id_ = id_.get('data-item-card')
            else:
                continue

            # Chequear si el ID ya fue scrapeado
            if id_ in seen_ids:
                consecutive_duplicates += 1
                if consecutive_duplicates >= max_consecutive_duplicates:
                    print(f"✓ Encontrados {max_consecutive_duplicates} duplicados consecutivos. Deteniendo scraping.")
                    break
                continue
            else:
                consecutive_duplicates = 0
                seen_ids.add(id_)
                page_has_new_data = True

            link = 'argenprop.com' + listing.find('a').get('href')
            direccion = listing.find(class_="card__address").text.strip()
            titulo = listing.find(class_="card__title").text.strip()

            # Extraer ambientes del título
            ambientes = np.nan
            titulo_lower = titulo.lower()
            
            # Mapeo de números en letras a dígitos
            numeros_letras = {
                'uno': 1, 'dos': 2, 'tres': 3, 'cuatro': 4, 'cinco': 5,
                'seis': 6, 'siete': 7, 'ocho': 8, 'nueve': 9, 'diez': 10
            }
            
            # Primero buscar números digitales
            match = re.search(r'(\d+)\s*(?:amb|ambiente|ambientes)', titulo_lower)
            if match:
                ambientes = int(match.group(1))
            else:
                # Buscar números en letras
                patron_letras = r'(?:' + '|'.join(numeros_letras.keys()) + r')\s*(?:amb|ambiente|ambientes)'
                match = re.search(patron_letras, titulo_lower)
                if match:
                    numero_palabra = match.group(0).split()[0]
                    ambientes = numeros_letras.get(numero_palabra, np.nan)
            
            # Caso especial: monoambiente
            if pd.isna(ambientes) and 'monoambiente' in titulo_lower:
                ambientes = 1

            ubicacion = listing.find(class_="card__title--primary").text[28:].split(', ')

            # La ubicación es de tipo "Capital Federal, barrio" o "barrio, sub_barrio"
            if ubicacion[-1] == 'Capital Federal':
                barrio = ubicacion[0]
                sub_barrio = np.nan
            else:
                barrio = ubicacion[-1]
                sub_barrio = ubicacion[0]

            # Precio extraction
            precio_element = listing.find(class_="card__price")
            if precio_element:
                precio_text = precio_element.text.strip()
                # Clean the price text - remove currency symbols and extra spaces
                precio_text_clean = precio_text.replace('$', '').replace('.', '').replace('USD', '').strip()
                # Split by space and take the first numeric part
                precio_parts = precio_text_clean.split()
                if precio_parts:
                    precio = precio_parts[0]
                else:
                    precio = np.nan
            else:
                precio = np.nan

            moneda = listing.find(class_="card__currency")
            moneda = moneda.text.strip() if moneda else np.nan

            expensas = listing.find(class_="card__expenses")
            expensas = expensas.text.strip() if expensas else np.nan

            # FIXED: Superficie extraction with new HTML structure
            superficie_element = listing.find(class_="basico1-icon-superficie_cubierta")
            if superficie_element:
                # Find the parent element that contains both the icon and the span
                parent_element = superficie_element.find_parent()
                if parent_element:
                    # Find the span that contains the superficie text
                    superficie_span = parent_element.find('span')
                    if superficie_span:
                        superficie = superficie_span.text.strip()
                    else:
                        superficie = np.nan
                else:
                    superficie = np.nan
            else:
                superficie = np.nan

            # FIXED: Dormitorios extraction with new HTML structure
            dormitorios_element = listing.find(class_="basico1-icon-cantidad_dormitorios")
            if dormitorios_element:
                # Find the parent element that contains both the icon and the span
                parent_element = dormitorios_element.find_parent()
                if parent_element:
                    # Find the span that contains the dormitorios text
                    dormitorios_span = parent_element.find('span')
                    if dormitorios_span:
                        dormitorios = dormitorios_span.text.strip()
                    else:
                        dormitorios = np.nan
                else:
                    dormitorios = np.nan
            else:
                dormitorios = np.nan

            data.append([id_, link, direccion, titulo, barrio, sub_barrio, precio, moneda, expensas, superficie, dormitorios, ambientes])

        # Si detectamos demasiados duplicados, parar el scraping
        if consecutive_duplicates >= max_consecutive_duplicates:
            break

    # Creo el df a partir de la lista de listas "data".
    if data:  # Check if there's any data
        df = pd.DataFrame(data=data, columns='id,link,direccion,titulo,barrio,sub_barrio,precio,moneda,expensas,superficie,dormitorios,ambientes'.split(','))

        # Agregar fecha y hora de scrape
        df['fecha_scrape'] = datetime.datetime.today()

        # Debug: Show values before cleaning
        print("DEBUG - Dormitorios values before cleaning:")
        print(df['dormitorios'].head(10))
        print("DEBUG - Superficie values before cleaning:")
        print(df['superficie'].head(10))
        print("DEBUG - Ambientes values:")
        print(df['ambientes'].head(10))

        # Limpieza
        # Precio cleaning
        df.precio = df.precio.apply(lambda x: str(x).replace('.', '').replace(',', '').strip() if pd.notna(x) else np.nan)
        df.precio = pd.to_numeric(df.precio, errors='coerce').astype(pd.Int64Dtype())
        
        df.expensas = df.expensas.apply(lambda x:x.split('\n')[0].split('$')[-1].replace('.', '') if type(x) == str else np.nan).astype(float).astype(pd.Int64Dtype())  # noqa: E721
        
        # FIXED: Superficie processing for new format
        df.superficie = df.superficie.apply(lambda x: x.split(' ')[0] if isinstance(x, str) else np.nan)
        df.superficie = df.superficie.astype(str)
        df.superficie = df.superficie.str.replace(',', '.')
        df.superficie = pd.to_numeric(df.superficie, errors='coerce')
        
        # FIXED: Dormitorios processing for new format (extract number before "dorm.")
        df.dormitorios = df.dormitorios.apply(lambda x: x.split(' ')[0] if isinstance(x, str) else np.nan)
        df.dormitorios = df.dormitorios.astype(str)
        df.dormitorios = pd.to_numeric(df.dormitorios, errors='coerce')
        df.dormitorios = df.dormitorios.astype(pd.Int64Dtype())
        
        # Ambientes processing
        df.ambientes = df.ambientes.astype(pd.Int64Dtype())

        # Resultado final
        print("Datos después de limpieza:")
        print(df[['dormitorios', 'superficie', 'precio', 'ambientes']].tail())

        # Guardar en nuevo archivo CSV
        df.to_csv(nombre_archivo_datos, index=False)
        print(f"✓ Datos guardados en: {nombre_archivo_datos}")
        print(f"✓ Total de publicaciones scrapeadas: {len(df)}")
        print(f"✓ Columnas: {list(df.columns)}")

    else:
        print("No se encontraron datos para guardar.")

    return
    
if __name__ == '__main__':
    main()

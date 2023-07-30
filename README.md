# BG_ETL
Repositorio de Black Glacier

###### Instrucciones para instalación
Se recomienda utilizar un ambiente de anaconda disponible en el repositorio
Revise como instalar anaconda en la página oficial: https://anaconda.org/
Una vez instalado anaconda, vaya a anaconda prompt o la terminal de código y ejecute en el directorio de BlackGlacier
```
conda env create -f bg_env.yml
conda activate blackglacier
pip install -r requirements.txt
```
Una vez instalado, para extraer la reflectancia de los productos ejecute 
```
python satellite_retrieval/Reflectance_extraction.py
```
Y para ejecutar la meteorologia, ejecute 
```
python meteo_retrieval/stormglass_connection.py
```
Considere que debe tener una key meteorológica de stormglass (https://stormglass.io/)

Hay que considerar también que este repositorio funciona con Google Earth Engine. El sistema está hecho para que se utilice un json de service account, ejemplo que se usa en `auth/AuthGee.py`. El json `auth/your.json` debe ser rellenado con la información que obtengas de lo que indica el tutorial oficial de https://developers.google.com/earth-engine/guides/service_account

#ADVERTENCIA: EL ARCHIVO JSON ES UN ARCHIVO SENSIBLE AL USO DE TUS CUENTAS, POR LO QUE PUEDES LIBERAR INFORMACIÓN PERSONAL A OTROS USUARIOS, BAJO NINGÚN MOTIVO SUBAS PÚBLICAMENTE ESA INFORMACIÓN

# BG_ETL
Repositorio de Black Glacier

######instrucciones para instalación
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

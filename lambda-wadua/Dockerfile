# Imagen base de lambda de Python 3.12
# La versión de Python se puede cambiar según la versión propia.
 FROM public.ecr.aws/lambda/python:3.12 

# Copiar requirements.txt al directorio raíz del contenedor.
 COPY requirements.txt ./ 

# Instalar dependencias desde requirements en el directorio raíz.
 RUN pip3 install -r ./requirements.txt 

# Copiar el código de la función al contenedor.
 COPY lambda_function.py ./ 

# Configurar el CMD con el manejador. file_name.function_name
 CMD [ "lambda_function.lambda_handler" ]
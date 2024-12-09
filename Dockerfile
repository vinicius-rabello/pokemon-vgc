FROM python:3.12

WORKDIR /pokemon_vgc_analysis

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "etl/main.py"]
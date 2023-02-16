FROM iqtlabs/edgetech-core:latest
COPY pyproject.toml .
COPY poetry.lock .
RUN pip3 install poetry==1.1.5
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev
COPY ./couchdb_saver.schema .
ADD ./couchdb_saver_pub_sub.py .
ENTRYPOINT [ "python3", "couchdb_saver_pub_sub.py" ]

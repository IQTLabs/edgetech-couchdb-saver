FROM iqtlabs/edgetech-core:latest
COPY ./requirements.txt .
RUN pip3 install -r requirements.txt
ADD ./couchdb_saver_pub_sub.py .
ENTRYPOINT [ "python3", "couchdb_saver_pub_sub.py" ]

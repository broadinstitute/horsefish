FROM us.gcr.io/broad-dsp-gcr-public/terra-jupyter-base:0.0.8
USER root
#this makes it so pip runs as root, not the user
ENV PIP_USER=false

RUN apt-get update && apt-get install -yq --no-install-recommends \
  python3.7-dev \
  python-tk \
  tk-dev \
  libssl-dev \
  xz-utils \
  libhdf5-dev \
  openssl \
  make \
  liblzo2-dev \
  zlib1g-dev \
  libz-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN pip3 -V \
 && pip3 install --upgrade pip \
 && pip3 install numpy==1.15.2 \
 && pip3 install py4j==0.10.7 \
 && python3 -mpip install matplotlib==3.0.0 \
 && pip3 install pandas==0.25.3 \
 && pip3 install pandas-gbq==0.12.0 \
 && pip3 install pandas-profiling==2.4.0 \
 && pip3 install seaborn==0.9.0 \
 && pip3 install notebook==5.7.8 \
 && pip3 install python-lzo==1.12 \
 && pip3 install google-cloud-bigquery==1.23.1 \
 && pip3 install google-api-core==1.6.0 \
 && pip3 install google-cloud-bigquery-datatransfer==0.4.1 \
 && pip3 install google-cloud-datastore==1.10.0 \
 && pip3 install google-cloud-resource-manager==0.30.0 \
 && pip3 install google-cloud-storage==1.23.0 \
 && pip3 install tenacity==6.0.0 \
 && pip3 install --upgrade git+git://github.com/broadinstitute/horsefish.git

ENV USER jupyter-user
USER $USER
#we want pip to install into the user's dir when the notebook is running
ENV PIP_USER=true

# Note: this entrypoint is provided for running Jupyter independently of Leonardo.
# When Leonardo deploys this image onto a cluster, the entrypoint is overwritten to enable
# additional setup inside the container before execution.  Jupyter execution occurs when the
# init-actions.sh script uses 'docker exec' to call run-jupyter.sh.
ENTRYPOINT ["/usr/local/bin/jupyter", "notebook"]
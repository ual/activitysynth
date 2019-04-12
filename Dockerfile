FROM continuumio/miniconda3

WORKDIR /home


ENV ACCESS_VAR_MODE compute
ENV CONDA_DIR /opt/conda
ENV PATH $CONDA_DIR/bin:$PATH
ENV CONDA_ENV usim

ENV PATH /opt/conda/envs/$CONDA_ENV/bin:$PATH
ARG AWS_ACCESS_KEY_ID
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

RUN conda update conda
RUN conda create --quiet --yes --channel conda-forge -p $CONDA_DIR/envs/$CONDA_ENV \
	python=3.6 \
	pip \
	pyarrow==0.12.1\
	choicemodels \
	urbansim_templates \
	s3fs

RUN git clone https://github.com/ual/activitysynth.git \
	&& cd activitysynth \
	&& $CONDA_DIR/envs/$CONDA_ENV/bin/python setup.py install

RUN conda config --add channels udst
RUN conda config --add channels conda-forge
RUN conda install --quiet --yes -p $CONDA_DIR/envs/$CONDA_ENV -c udst pandana 

ENV year 2011
WORKDIR /home/activitysynth/activitysynth
ENTRYPOINT $CONDA_DIR/envs/$CONDA_ENV/bin/python run.py -d s3 -y $year

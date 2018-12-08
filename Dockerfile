#
# transportation-dockless-processing
#
FROM python:3.6

RUN apt-get update && apt-get install dialog apt-utils python3-rtree curl -y

#  Set the working directory
WORKDIR /app

#  Copy package requirements
COPY requirements.txt /app

#  Update python3-pip
RUN python -m pip install pip --upgrade

#  Install python packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt
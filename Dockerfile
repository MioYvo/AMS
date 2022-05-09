FROM registry.cn-hangzhou.aliyuncs.com/mio101/amzlinux-python3:3.10

ENV TZ='Asia/Shanghai'

COPY requirements.txt /app/ams/
WORKDIR /app/ams

RUN rpm --rebuilddb && \
    yum update -y && \
    yum install -y yum-utils gcc && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    pip3 install --no-cache-dir -r requirements.txt && \
    pip3 cache purge && \
    package-cleanup -q --leaves --all | xargs -l1 yum -y remove && \
    yum -y autoremove && \
    yum clean all && \
    rm -rf /var/cache/yum && \
    find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf

COPY app /app/ams/app
COPY core /app/ams/core
COPY *.py /app/ams/
COPY settings.toml /app/ams/

USER nobody

CMD ["python3", "-u", "main.py"]
# influx-to-s3

## 备份到 S3

### 1. 安装依赖（Ubuntu 操作系统）

```
sudo apt update
sudo apt install python3
sudo apt install python3-pip
python3 -m pip install requests
python3 -m pip install influxdb_client
python3 -m pip install boto3
```

安装 mountpoint-s3

```
wget https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.deb
sudo apt-get install -y ./mount-s3.deb
```

安装 aws cli

```
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
sudo apt install unzip
unzip awscliv2.zip
sudo ./aws/install
```

### 2. 备份数据到 S3

首先配置环境变量

```
export INFLUX_SRC_TOKEN="源InfluxDB的token"
```

确保实例可以对目标 S3 桶进行读写

#### 方式一： 以 tsm 格式备份

##### 1) 备份单个 bucket

```
python3 backup.py --src-org 要备份的bucket所在org的名称 --src-bucket 要备份的bucket的名称 --src-host http://localhost:8086 --s3-bucket S3桶的名称 --log-level debug
```

##### 2) Full Backup

以下指令会备份所有 tokens, users, buckets, dashboards

```
python3 backup.py --full --confirm-full  --src-host http://localhost:8086 --s3-bucket S3桶的名称 --log-level debug
```

#### 方式二： 以 csv 格式备份

##### 1) 备份单个 bucket

```
python3 backup.py --src-org 要备份的bucket所在org的名称 --src-bucket 要备份的bucket的名称 --src-host http://localhost:8086 --s3-bucket S3桶的名称 --csv --log-level debug
```

##### 2) 备份所有 bucket

备份所有 org 中的所有用户定义的 buckets

```
python3 backup.py --full --src-host http://localhost:8086 --s3-bucket S3桶的名称 --csv --log-level debug
```

### 3. 还原数据

首先配置环境变量

```
export INFLUX_DEST_TOKEN="源InfluxDB的token"
```

确保实例可以对目标 S3 桶进行读写

#### 3.1 还原数据

#### 方式一：从 tsm 备份还原数据

##### 1) 还原单个 bucket

```
python3 restore.py --src-bucket 源bucket的名称 --dest-bucket 目的bucket的名称 --dest-org 目标org名称 --s3-bucket S3桶的名称 --dest-host https://目标influxdb数据库的地址:8086 --retry-restore-dir influxdb-backups/s3桶中的某次tsm备份文件夹名称 --log-level debug
```

##### 2) 还原所有数据（会完全覆盖目标数据库）

```
python3 restore.py --full --confirm-full --s3-bucket S3桶的名称 --dest-host https://目标influxdb数据库的地址:8086 --retry-restore-dir influxdb-backups/s3桶中的某次tsm备份文件夹名称 --log-level debug
```

#### 方式二：从 csv 备份还原数据

##### 1) 还原单个 bucket

```
python3 restore.py --src-org 源org的名称 --dest-org 目的org的名称 --src-bucket 源bucket的名称 --dest-bucket 目的bucket的名称 --s3-bucket s3桶的名称 --dest-host https://目标influxdb数据库的地址:8086 --retry-restore-dir influxdb-backups/s3桶中的某次csv备份文件夹名称  --csv --log-level debug
```

##### 2) 还原所有 buckets

```
python3 restore.py --full --confirm-full --s3-bucket S3桶的名称 --dest-host https://目标influxdb数据库的地址:8086 --retry-restore-dir influxdb-backups/s3桶中的某次csv备份文件夹名称 --csv --log-level debug
```

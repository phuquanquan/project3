from hdfs import InsecureClient

# TODO: update with your HDFS configuration
HDFS_URL = "http://<namenode_host>:<port>"

client = InsecureClient(HDFS_URL)


def write_to_hdfs(file_path: str, destination: str):
    with open(file_path, "rb") as f:
        client.write(destination, data=f, overwrite=True)

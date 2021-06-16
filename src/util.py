
from dask.distributed import Client, LocalCluster, get_client

def get_dask_client(n_workers=3):
    #  just until we can get some better equipment
    try:
        return get_client()
    except ValueError:
        cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1)
        return Client(cluster)

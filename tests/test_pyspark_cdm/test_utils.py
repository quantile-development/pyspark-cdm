from pyspark_cdm.utils import remove_root_from_path


def test_remove_root_from_path():
    """
    Make sure that the root is removed from the path.
    """
    assert remove_root_from_path("/dbfs/mnt/test/", "/dbfs") == "/mnt/test/"
    assert remove_root_from_path("/dbfs/mnt/test/", "/something") == "/dbfs/mnt/test/"

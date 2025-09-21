from utilities import OCIFSManager

ocifs = OCIFSManager(profile="prof")
f = ocifs.open(bucket="mosaic", prefix="my_prefix", filename="my_file.txt", mode="wb")
f.close()

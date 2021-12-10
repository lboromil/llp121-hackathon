import os

KB = 1024
MB = KB * 1000


def split_csv(file: str, out: str):
    # create output directory
    try:
        os.makedirs(out)
    except OSError:  # if exists, remove everything inside
        for fname in os.listdir(out):
            os.remove(os.path.join(out, fname))

    part = 0  # the part number of the chunk
    f = open(file, "rb")
    while True:
        chunk = f.read(int(50 * MB))
        if not chunk:
            break
        part += 1
        fname = os.path.join(out, (f"global_food_prices.ext{part:1d}"))
        fobj = open(fname, "wb")
        fobj.write(chunk)
        fobj.close()
    f.close()

    # making sure it didn't explode
    assert part <= 9999

    # try to remove the file since not needed in push (can always been retrieved with merge)
    try:
        os.remove(file)
    except OSError:
        pass


def merge_csv_chunks(folder: str, out: str):
    output = open(out, "wb")
    files = os.listdir(folder)
    files.sort()
    for fname in files:
        fpath = os.path.join(folder, fname)
        fobj = open(fpath, "rb")
        while 1:
            fbytes = fobj.read(KB)
            if not fbytes:
                break
            output.write(fbytes)
        fobj.close()
    output.close()


if __name__ == "__main__":
    # test
    split_csv(
        os.path.abspath("../datasets/global_food_prices.csv"),
        os.path.abspath("../datasets/global_food_prices"),
    )
    merge_csv_chunks(
        os.path.abspath("../datasets/global_food_prices"),
        os.path.abspath("../datasets/global_food_prices.csv"),
    )


import gzip

def read_file_text(file_name):
    # read and return the contents of a text file
    with open(file_name,"r") as file:
        text=file.read().strip()
    return text

def gzip_file(file_name):
    gz_file=file_name+'.gz'
    with open(file_name, "rb") as file_in:
        with gzip.open(gz_file, "wb") as file_out:
            file_out.writelines(file_in)
    return gz_file

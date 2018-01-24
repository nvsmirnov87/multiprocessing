#coding: utf-8

from multiprocessing import Pool, Lock, Manager
import os
from random import choice, randint
from string import ascii_letters
from time import time
from zipfile import ZipFile
from functools import partial
from sys import version as py_version, exit as err_exit
import uuid


# Get tuple of unique ids.
def get_ids(count_zips, count_xmlfile):
    ids_count = count_zips * count_xmlfile  # Get count of files (ids)
    list_ids = list()
    # Get the set of unique ids
    for _ in range(ids_count):
        list_ids.append(uuid.uuid4().hex)
    tuple_ids = tuple(list_ids)
    return tuple_ids


# Write XML files to Zip archive
def create_zip(tuple_ids, path, count_xmlfile, zip_no):
    with ZipFile(os.path.join(path, 'Zip_' + str(zip_no) + '.zip'), 'w') as z:
        for i in range(count_xmlfile):
            file_name = 'XMLfile_{}_{}.xml'.format(str(zip_no), str(i))
            list_str = list()
            list_str.append("<root>")
            list_str.append("\t<var name='id' value='{}'/>".format(
                tuple_ids[zip_no * count_xmlfile + i]))
            list_str.append("\t<var name='level' value='{}'/> ".
                            format(randint(1, 100)))
            list_str.append("\t<objects>")

            for j in range(randint(1, 10)):
                list_str.append("\t\t<object name='{}'/>".format(
                    ''.join(choice(ascii_letters) for _ in range(
                        randint(5, 30)))))
            list_str.append("\t</objects>")
            list_str.append("</root>")
            xml_file = "\n".join((_ for _ in list_str))
            z.writestr(file_name, xml_file)


# Parse zip-archive. Get id, level, objects and write them into the csv-files.
def parse_zip(lock, path, out_csv1, out_csv2, name_zip):
    with ZipFile(os.path.join(path, name_zip), 'r') as z:
        list_of_files_in_zip = z.namelist()

        # parse zip file and get id, level, objects values
        id_level_objects = []
        for fname in list_of_files_in_zip:
            list_of_object = []
            for string in z.read(fname).decode("utf-8").split('\n'):
                if "name='id'" in string:
                    idp = string.split("id' value='")[1].split("'")[0]
                if "name='level'" in string:
                    level = string.split("'level' value='")[1].split("'")[0]
                if "object name='" in string:
                    list_of_object.append(string.split("object name='")[1].
                                          split("'")[0])
            id_level_objects.append([idp, level, list_of_object])

        # write id, level, objects into .csv-files
        lock.acquire()
        with open(out_csv1, "a") as file1:
            for i in range(len(list_of_files_in_zip)):
                file1.write(id_level_objects[i][0] + ','
                            + id_level_objects[i][1] + '\n')

        with open(out_csv2, "a") as file2:
            for i in range(len(list_of_files_in_zip)):
                for my_object in id_level_objects[i][2]:
                    file2.write(id_level_objects[i][0] + ',' + my_object + '\n')
        lock.release()


if __name__ == '__main__':
    if py_version[0] != '3':
        err_exit('!!! Use Python3 instead Python2 to run the programm')

    path = ''
    while os.path.exists(path) is False:
        path = input("Input correct path to working directory, please\n")

    out_csv1 = os.path.join(path, 'csv1.csv')
    out_csv2 = os.path.join(path, 'csv2.csv')
    count_zips = 50
    count_xmlfile = 100

    # First task: create ZIPs archives with XML files
    t1 = time()
    tuple_ids = get_ids(count_zips, count_xmlfile)  # get list of string id
    func = partial(create_zip, tuple_ids, path, count_xmlfile)
    p = Pool()
    p.imap_unordered(func, range(count_zips))
    p.close()
    p.join()
    print('Create .zip files time = {}s'.format(str(time() - t1)))

    # Second task: get id, level, objects from .zip and write them to .csv.
    t1 = time()
    with open(out_csv1, "w") as file1:
        file1.write("id,level\n")
    with open(out_csv2, "w") as file2:
        file2.write("id,object_name\n")
    tuple_of_zips = (f for d, dirs, files in os.walk(path)
                        for f in files if ".zip" in f)
    lock = Lock()
    p = Pool()
    m = Manager()  # Manager is needed to distribute Lock to all processes
    lock = m.Lock()
    func = partial(parse_zip, lock, path, out_csv1, out_csv2)
    p.imap_unordered(func, tuple_of_zips)
    p.close()
    p.join()
    print('Create .csv files time = {}s'.format(str(time() - t1)))

#coding: utf-8

from multiprocessing import Pool, Lock, Manager
import os
from random import choice, randint
from string import ascii_letters
from time import time
from zipfile import ZipFile
from functools import partial


# Get tuple of unique ids.
def get_ids(count_zips, count_xmlfile):
    ids_count = count_zips * count_xmlfile  # Get count of files (ids)
    set_for_find_repeat = set()  # The set is used for quick search

    # Get the set of unique ids
    while len(set_for_find_repeat) < ids_count:
        id = ''.join(choice(ascii_letters) for i in range(15))
        if id not in set_for_find_repeat:
            set_for_find_repeat.add(id)
    return tuple(set_for_find_repeat)


# Write XML files to Zip archive
def create_zip(tuple_ids, path, count_xmlfile, zip_no):
    with ZipFile(os.path.join(path, 'Zip_' + str(zip_no) + '.zip'), 'w') as z:
        for i in range(count_xmlfile):
            file_name = 'XMLfile_' + str(zip_no) + '_' + str(i) + ".xml"
            stroka = "<root>\n\t<var name='id' value='%s'/>\n\t<var name='level' value='%s'/> \n\t<objects>\n" \
                     % (tuple_ids[zip_no*count_xmlfile+i], randint(1, 100))
            for j in range(randint(1, 10)):
                stroka += "\t\t<object name='%s'/>\n" % (
                          ''.join(choice(ascii_letters) for k in range(randint(5, 30))))
            stroka += "\t</objects>\n</root>"
            z.writestr(file_name, stroka)


# Parse zip-archive. Get id, level, options and write them into the csv-files.
def parse_zip(lock, path, out_csv1, out_csv2, name_zip):
    with ZipFile(os.path.join(path, name_zip), 'r') as z:
        list_of_files_in_zip = z.namelist()

        # parse zip file and get id, level, options values
        id_level_objects = []
        for fname in list_of_files_in_zip:
            list_of_object = []
            for string in z.read(fname).decode("utf-8").split('\n'):
                if "name='id'" in string:
                    idp = string.split("id' value='")[1].split("'")[0]
                if "name='level'" in string:
                    level = string.split("'level' value='")[1].split("'")[0]
                if "object name='" in string:
                    list_of_object.append(string.split("object name='")[1].split("'")[0])
            id_level_objects.append([idp, level, list_of_object])

        # write id, level. options into .csv-files
        lock.acquire()
        with open(out_csv1, "a") as file1:
            for i in range(len(list_of_files_in_zip)):
                file1.write(id_level_objects[i][0] + ',' + id_level_objects[i][1] + '\n')

        with open(out_csv2, "a") as file2:
            for i in range(len(list_of_files_in_zip)):
                for my_object in id_level_objects[i][2]:
                    file2.write(id_level_objects[i][0] + ',' + my_object + '\n')
        lock.release()


if __name__ == '__main__':
    path = ''
    while os.path.exists(path) is False:
        path = input("Input correct path to working directory, please\n")
    out_csv1 = os.path.join(path, 'csv1.csv')
    out_csv2 = os.path.join(path, 'csv2.csv')

    # First task: create ZIPs archives with XML files
    t1 = time()
    count_zips = 50
    count_xmlfile = 100
    tuple_ids = get_ids(count_zips, count_xmlfile)  # get list of string id
    func = partial(create_zip, tuple_ids, path, count_xmlfile)
    p = Pool()
    p.imap_unordered(func, range(count_zips))
    p.close()
    p.join()
    print('Create .zip files time = {}s'.format(str(time() - t1)))

    # Second task: grep id, level, options from .zip and write them to .csv files
    t1 = time()
    with open(out_csv1, "w") as file1:
        file1.write("id" + ',' + "level" + '\n')
    with open(out_csv2, "w") as file2:
        file2.write("id" + ',' + "object_name" + '\n')
    tuple_of_zips = (f for d, dirs, files in os.walk(path) for f in files if ".zip" in f)
    lock = Lock()
    p = Pool()
    m = Manager()  # Manager is needed to distribute Lock to all processes
    lock = m.Lock()
    func = partial(parse_zip, lock, path, out_csv1, out_csv2)
    p.imap_unordered(func, tuple_of_zips)
    p.close()
    p.join()
    print('Create .csv files time = {}s'.format(str(time() - t1)))

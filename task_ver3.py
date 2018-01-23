#coding: utf-8

from multiprocessing import Pool, Lock
import os
from random import choice, randint
from string import ascii_letters
from time import time
from zipfile import ZipFile
from sys import version as py_version


class GetZips(object):

    """ Create zips with .xml files. """

    def __init__(self, path):
        self.count_zips = 50
        self.count_XMLfile = 100
        self.tuple_ids = ()
        self.path = path

    # Get tuple of unique ids.
    def get_ids(self):
        ids_count = self.count_zips * self.count_XMLfile  # Get count of files
        set_for_find_repeat = set()  # The set is used for quick search

        # Get the set of unique ids
        while len(set_for_find_repeat) < ids_count:
            id = ''.join(choice(ascii_letters) for __ in range(15))
            if id not in set_for_find_repeat:
                set_for_find_repeat.add(id)
        self.tuple_ids = tuple(set_for_find_repeat)

    # Write XML files to Zip archive
    def create_zip(self, zip_no):
        with ZipFile(os.path.join(self.path, 'Zip_' + str(zip_no) + '.zip'),
                     'w') as z:
            for i in range(self.count_XMLfile):
                file_name = 'XMLfile_' + str(zip_no) + '_' + str(i) + ".xml"
                stroka = "<root>\n\t<var name='id' value='%s'/>\n\t<var name=" \
                         "'level' value='%s'/> \n\t<objects>\n" \
                         % (self.tuple_ids[zip_no*self.count_XMLfile+i],
                            randint(1, 100))
                for j in range(randint(1, 10)):
                    stroka += "\t\t<object name='%s'/>\n" % (
                              ''.join(choice(ascii_letters)
                                      for __ in range(randint(5, 30))))
                stroka += "\t</objects>\n</root>"
                z.writestr(file_name, stroka)

    # Start function.  Pool of processes for creating Zip archives.
    def create_zips(self):
        t1 = time()
        self.get_ids()  # Get tuple of string id
        p = Pool()
        p.imap_unordered(self.create_zip, range(self.count_zips))
        p.close()
        p.join()
        print('Create .zip files time = {}s'.format(str(time() - t1)))


class GetCsv(object):

    """ Parse .xml from .zip.  Create .csv with results. """

    def __init__(self, path):
        self.path = path
        self.out_csv1 = os.path.join(self.path, 'csv1.csv')
        self.out_csv2 = os.path.join(self.path, 'csv2.csv')

    # Parse zip-archive. Get id, level, objects and write them into the .csv.
    def parse_zip(self, name_zip):
        with ZipFile(os.path.join(self.path, name_zip), 'r') as z:
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
            with open(self.out_csv1, "a") as file1:
                for i in range(len(list_of_files_in_zip)):
                    file1.write(id_level_objects[i][0] + ',' +
                                id_level_objects[i][1] + '\n')

            with open(self.out_csv2, "a") as file2:
                for i in range(len(list_of_files_in_zip)):
                    for my_object in id_level_objects[i][2]:
                        file2.write(id_level_objects[i][0] + ',' + my_object +
                                    '\n')
            lock.release()

    # create .csv files.
    def create_csv(self):
        t1 = time()
        with open(self.out_csv1, "w") as file1:
            file1.write("id" + ',' + "level" + '\n')
        with open(self.out_csv2, "w") as file2:
            file2.write("id" + ',' + "object_name" + '\n')
        gen_list_of_zips = (f for d, dirs, files in os.walk(self.path)
                            for f in files if ".zip" in f)
        p = Pool()
        p.imap_unordered(self.parse_zip, gen_list_of_zips)
        p.close()
        p.join()
        print('Create .csv files time = ' + str(time() - t1) + 's')


if __name__ == '__main__':
    if py_version[0] == '3':
        lock = Lock()
        path = ''
        while os.path.exists(path) is False:
            path = input("Input correct path to working directory, please\n")

        # First task: create ZIPs archives with XML files
        A = GetZips(path)
        A.create_zips()

        # Second task: get id, level, objects from .zip and write them to .csv
        B = GetCsv(path)
        B.create_csv()
    else:
        print("\n!!! Use Python3 instead Python2 to run the programm\n")

#-*- coding: utf-8-*-
from multiprocessing import Pool
import os
from random import choice, randint
from string import ascii_letters
from time import time
from zipfile import ZipFile
from sys import version as py_version, exit as err_exit
import uuid


class GetZips(object):

    """ Create zips with .xml files. """

    def __init__(self, path):
        self.count_zips = 50
        self.count_XMLfile = 100
        self.tuple_ids = None
        self.path = path

    # Get tuple of unique ids.
    def get_ids(self):
        ids_count = self.count_zips * self.count_XMLfile  # Get count of files
        list_ids = list()
        # Get the set of unique ids
        for _ in range(ids_count):
            list_ids.append(uuid.uuid4().hex)
        self.tuple_ids = tuple(list_ids)

    # Write XML files to Zip archive
    def create_zip(self, zip_no):
        with ZipFile(os.path.join(self.path, 'Zip_{}.zip'.format(zip_no)),
                     'w') as z:
            for i in range(self.count_XMLfile):
                file_name = 'XMLfile_{}_{}.xml'.format(str(zip_no), str(i))
                list_str = list()
                list_str.append("<root>")
                list_str.append("\t<var name='id' value='{}'/>".format(
                    self.tuple_ids[zip_no * self.count_XMLfile + i]))
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
        self.res = None

    # Parse zip-archive.  Get id, level, objects and write them into csv-files.
    def parse_zip(self, zip_name):
        with ZipFile(os.path.join(self.path, zip_name), 'r') as z:
            list_of_files_in_zip = z.namelist()

            # Parse zip file and get id, level, objects values
            id_level_objects = []
            for fname in list_of_files_in_zip:
                list_of_object = list()
                for string in z.read(fname).decode("utf-8").split('\n'):
                    if "name='id'" in string:
                        id = string.split("id' value='")[1].split("'")[0]
                    if "name='level'" in string:
                        level = string.split("'level' value='")[1].split("'")[0]
                    if "object name='" in string:
                        list_of_object.append(string.split("object name='")[1].
                                              split("'")[0])
                id_level_objects.append((id, level, list_of_object))

        return id_level_objects

    def write_csv(self, q_nom):
        if q_nom == 1:
            # Write id, level values into .csv-files
            write_part = 'id, level\n'

            for part in ((j[0], j[1]) for i in self.res for j in i):
                write_part += (part[0] + ',' + part[1] + '\n')
        else:
            # Write id, object values into .csv-files
            write_part = 'id, object_name\n'
            for part in ((j[0], j[2]) for i in self.res for j in i):
                for obj in part[1]:
                    write_part += (part[0] + ',' + obj + '\n')
        with open(os.path.join(self.path, 'csv' + str(q_nom) + '.csv'), "w") \
                as file:
                file.write(write_part)

    # create .csv files.
    def create_csv(self):
        t1 = time()
        # Get name of zip files
        tuple_of_zips = (f for d, dirs, files in os.walk(self.path)
                         for f in files if ".zip" in f)
        # Parse zip filse
        p = Pool()
        self.res = p.map(self.parse_zip, tuple_of_zips)
        p.close()
        p.join()
        # Create csv files
        p = Pool()
        p.imap_unordered(self.write_csv, (1, 2))
        p.close()
        p.join()
        print('Create .csv files time = {}s'.format(str(time() - t1)))


if __name__ == '__main__':
    if py_version[0] != '3':
        err_exit('!!! Use Python3 instead Python2 to run the programm')

    path = ''
    while os.path.exists(path) is False:
        path = input("Input correct path to working directory, please\n")

    # First task: create ZIPs archives with XML files
    A = GetZips(path)
    A.create_zips()

    # Second task: get id, level, objects from .zip and write them to .csv
    B = GetCsv(path)
    B.create_csv()

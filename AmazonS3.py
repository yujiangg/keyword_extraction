import boto3, json, os, logging
from botocore.exceptions import ClientError
import datetime
import pickle
import re
from pathlib import Path
from definitions import ROOT_DIR
from basic import datetime_to_str, logging_channels, filterListofDictByDict, timing, to_datetime

class AmazonS3 :
    def __init__(self, bucket_name='elephants3', settings_filename='s3_settings.json'):
        self.bucket_name = bucket_name
        self.settings = self._LoadConfig(settings_filename)
        aws_connector = boto3.Session(
                                  aws_access_key_id=self.settings["access_key"],
                                  aws_secret_access_key=self.settings["access_secret"],
                                  region_name=self.settings["region_name"])
        s3 = aws_connector.resource("s3")
        self._bucket = s3.Bucket(bucket_name)

    def downloadTrackerData(self, date):
        objects = self.getDateObjects(date, prefix_path="tracker")
        n_obj = awsS3._CountObejects(objects)
        for i, object in enumerate(objects, 1):
            path_object = object.key
            self.download_file(path_object)
            print(f"finish loading number of objects, {i}/{n_obj}")

    def dumpDateDataFilter(self, date, dict_criteria={'event_type': None,'web_id': None},
                           pattern="%Y-%m-%d", prefix_path=None):
        data_list_filter = self.getDateDataFilter(date, dict_criteria, prefix_path)
        root_folder = datetime_to_str(to_datetime(date, pattern), pattern="%Y/%m/%d")
        path_folder = os.path.join(ROOT_DIR, "s3data", root_folder)
        filename = "rawData.pickle"
        # path_write = f"{ROOT_DIR}/s3data/{root_folder}/rawData.pickle"
        self.PickleDump(data_list_filter, path_folder, filename)
        return data_list_filter

    def dumpDateHourDataFilter(self, date, hour, dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d"):
        data_list_filter = self.getDateHourDataFilter(date, hour, dict_criteria)
        root_folder = datetime_to_str(to_datetime(date, pattern), pattern="%Y/%m/%d")
        sub_folder = datetime_to_str(to_datetime(f'{date}-{hour}', "%Y-%m-%d-%H"), '%H')
        path_folder = os.path.join(ROOT_DIR, "s3data", root_folder, sub_folder)
        filename = "rawData.pickle"
        # path_write = f"{ROOT_DIR}/s3data/{root_folder}/{sub_folder}/rawHourData.pickle"
        self.PickleDump(data_list_filter, path_folder, filename)
        return data_list_filter

    def getDateDataFilter(self, date, dict_criteria={'event_type': None,'web_id': None}, prefix_path=None):
        data_list_filter = []
        objects = self.getDateObjects(date, prefix_path)
        n_obj = AmazonS3._CountObejects(objects)
        for i,object in enumerate(objects):
            path_object = object.key
            # print(path_object)  ##path of each Object
            data_list = json.loads(self.Read(path_object))
            if i%10==0:
                print(f"finish loading number of objects, {i}/{n_obj}")
            data_list_filter += filterListofDictByDict(data_list, dict_criteria=dict_criteria)
        return data_list_filter

    def getDateHourDataFilter(self, date, hour, dict_criteria={'event_type': None,'web_id': None}, prefix_path=None):
        data_list_filter = []
        objects = self.getDateHourObjects(date, hour, prefix_path)
        n_obj = AmazonS3._CountObejects(objects)

        for i,object in enumerate(objects):
            path_object = object.key
            # print(path_object)  ##path of each Object
            data_list = json.loads(self.Read(path_object))
            if i%10==0:
                print(f"finish loading number of objects, {i}/{n_obj}")
            # print(f"finish loading {path_object}")
            data_list_filter += filterListofDictByDict(data_list, dict_criteria=dict_criteria)
        return data_list_filter
    def getDateObjects(self, date, prefix_path=None):
        ## precision to hour
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
        date = datetime_to_str(date, pattern="%Y/%m/%d")
        path_bucket = f"{date}"
        if prefix_path:
            path_bucket = f"{prefix_path}/{path_bucket}"
        return self._bucket.objects.filter(Prefix=path_bucket)

    def getDateHourObjects(self, date, hour, prefix_path=None):
        ## precision to hour
        date_hour = datetime.datetime.strptime(f"{date} {hour}", '%Y-%m-%d %H')
        datetime_hour = datetime_to_str(date_hour, pattern="%Y/%m/%d/%H")
        path_bucket = f"{datetime_hour}"
        if prefix_path:
            path_bucket = f"{prefix_path}/{path_bucket}"
        return self._bucket.objects.filter(Prefix=path_bucket)

    def getLatestHourObjects(self):
        ## precision to hour
        datetime_latest = datetime_to_str(datetime.datetime.utcnow(), pattern="%Y/%m/%d/%H")
        path_bucket = f"{datetime_latest}"
        return self._bucket.objects.filter(Prefix=path_bucket)

    ## quick test for unit test
    def getLatestMinObjects(self):
        ## precision to minute
        datetime_latest = datetime_to_str(datetime.datetime.utcnow(), pattern="%Y/%m/%d/%H")
        prefix = "elephanthorse-2"
        datetime_min_latest = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(minutes=1), pattern="%Y-%m-%d-%H-%M")
        path_bucket = f"{datetime_latest}/{prefix}-{datetime_min_latest}"
        return self._bucket.objects.filter(Prefix=path_bucket)

    @staticmethod
    def _CountObejects(Objects):
        counter = 0
        for object in Objects:
            if object.size != 0:
                counter += 1
        return counter

    def ListObjects(self):
        """ A function that listing all files in bucket """
        return self._bucket.objects.all()

    def ListSpecificObjects(self,path):
        """ A function that listing specific files in bucket """
        return self._bucket.objects.filter(Prefix=path)

    def PickleDump(self, data_list, path_foler, filename):

        Path(path_foler).mkdir(parents=True, exist_ok=True)
        path_write = os.path.join(path_foler, filename)
        with open(path_write, 'wb') as f:
            pickle.dump(data_list, f)

    def PickleLoad(self, path_read):
        with open(path_read, 'rb') as f:
            data_list = pickle.load(f)
        return data_list

    def JsonLoad(self, key):
        """ Reading the S3 object from bucket after json decode """
        try :
            return json.load(self._bucket.Object(key=key).get()["Body"])
        except :
            return False

    def JsonDump(self, key, obj):
        """ Writing the S3 object from bucket after json encode """
        try :
            return self._bucket.Object(key=key).put(Body=json.dumps(obj))
        except :
            return False

    def Read(self, key):
        """ Reading the S3 object from bucket """
        try:
            data_decode = self._bucket.Object(key=key).get()["Body"].read().decode().replace('}{', '},{')
            subDict = {r'}("landing")+{': '},{',
                       r'^("landing")+{': '{',
                       r'}("landing")+$': '}'}
            for k, v in subDict.items():
                data_decode = re.sub(k, v, data_decode)
            return '[' + data_decode + ']'
        except :
            return '[]'

    def Dump(self, key, obj):
        """ Writing the S3 object from bucket """
        try :
            return self._bucket.Object(key=key).put(Body=obj)
        except :
            return False

    def _LoadConfig(self, settings_filename):
        self.settings_path = os.path.join(ROOT_DIR, 's3_parser', settings_filename)
        with open(self.settings_path) as settings_file:
            settings = json.load(settings_file)
            return settings

    @logging_channels(['clare_test'])
    @timing
    def upload_tracker_data(self, datetime_utc0, s3_ROOT_DIR='tracker'):
        if type(datetime_utc0) == str:
            datetime_utc0 = datetime.datetime.strptime(datetime_utc0, "%Y-%m-%d %H:%M:%S")
        MID_DIR = datetime.datetime.strftime(datetime_utc0, format="%Y/%m/%d/%H")
        file_name = os.path.join(ROOT_DIR, "s3data", MID_DIR, "rawData.pickle") ## path in local
        object_name = os.path.join(s3_ROOT_DIR, MID_DIR, 'rawData.pickle') ## path in s3
        print(f"upload file from {file_name} to s3 {object_name}")
        self._upload_file(file_name, object_name)

    def download_file(self, object_name, file_path=None):
        """Download a file from an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :param file_path: str, path to save in local
        :return: True if file was downloaded, else False
        """

        if not file_path:
            file_path = object_name.split(os.sep)
            file_path[0] = "s3data"
            file_path = os.path.join(ROOT_DIR, os.sep.join(file_path))
        path_folder = os.sep.join(file_path.split(os.sep)[:-1])
        Path(path_folder).mkdir(parents=True, exist_ok=True)
        # path_write = os.path.join(path_foler, filename)
        s3_client = boto3.client('s3', aws_access_key_id=self.settings["access_key"],
                                  aws_secret_access_key=self.settings["access_secret"],
                                  region_name=self.settings["region_name"])
        try:
            with open(file_path, 'wb') as f:
                s3_client.download_fileobj(self.bucket_name, object_name, f)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    def _upload_file(self, file_name, object_name=None):
        """Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        s3_client = boto3.client('s3', aws_access_key_id=self.settings["access_key"],
                                  aws_secret_access_key=self.settings["access_secret"],
                                  region_name=self.settings["region_name"])
        try:
            response = s3_client.upload_file(file_name, self.bucket_name, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

## unit test
if __name__ == "__main__":
    ## download tracker data
    datetime_lastHour = datetime.datetime.utcnow()-datetime.timedelta(hours=0)
    date = datetime_to_str(datetime_lastHour, pattern="%Y-%m-%d")
    hour = datetime_to_str(datetime_lastHour, pattern="%H")
    s3 = AmazonS3('elephants3')
    # data_list_filter = s3.dumpDateHourDataFilter(date, hour, dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d")

    data_list_filter = []
    objects = s3.getDateHourObjects(date, hour)
    n_obj = AmazonS3._CountObejects(objects)

    for i, object in enumerate(objects):
        if i != 1130:
            continue
        path_object = object.key
        # print(path_object)  ##path of each Object
        # x = s3._bucket.Object(key=path_object).get()["Body"].read().decode().replace('}{', '},{')
        # x = s3._bucket.Object(key=path_object).get()["Body"].read().decode().replace('}"landing"{', '},{')

        # x = s3._bucket.Object(key=path_object).get()["Body"].read().decode()

        # print(x[2529524-100:2529524+1000])
        # print(x[0:0+100])
        #
        # with open('test.txt', 'w') as file:
        #     file.write(x)

        str_x = s3._bucket.Object(key=path_object).get()["Body"].read().decode()

        data_list = json.loads(s3.Read(path_object))
        print(f"finish loading number of objects, {i}/{n_obj}")
        #
        # try:
        #     print(s3._bucket.Object(key=path_object).get()["Body"].read().decode().replace('}{', '},{'))
        #     data_list = json.loads(s3.Read(path_object))
        #
        # except:
        #
        #     # '[' + s3._bucket.Object(key=path_object).get()["Body"].read().decode().replace('}{', '},{') + ']'
        #     print(s3._bucket.Object(key=path_object).get()["Body"].read().decode().replace('}{', '},{'))

        # if i % 10 == 0:
        #     print(f"finish loading number of objects, {i}/{n_obj}")
        # print(f"finish loading {path_object}")
        # data_list_filter += filterListofDictByDict(data_list, dict_criteria={'event_type': None,'web_id': None})


    # objects = awsS3.getDateObjects('2022-08-01')
    # data = []
    # for i,obj in enumerate(objects):
    #     if i == 2: break
    #     print(obj)
    #     raw = json.loads(awsS3.Read(obj.key))
    #     data_filter = list(filter(lambda x: x['web_id'] == 'ctnews', raw))
    #     data.extend(data_filter)
    #
    #     # awsS3.download_file(obj.key, f's3data/{i}')
    #
    # data_filter = list(filter(lambda x: 'web_id' in x.keys(), data))
    # data_filter = list(filter(lambda x: x['web_id'] =='ctnews', data_filter))

    # awsS3.downloadTrackerData("2022-08-09")



    ## test for uploading
    # AmazonS3('elephants3')._upload_file('/home/clare/Desktop/parse_tracker/s3data/click_count_total.png', 'tracker')
    # path = '/home/clare/Desktop/parse_tracker/s3data/2022/01/27/00/rawData.pickle'
    # AmazonS3('elephants3').upload_tracker_data(datetime_utc0='2022-01-27 01:00:00')

    # data_list = AmazonS3('elephants3').getDateDataFilter('2022-02-16',{'behavior_type': 'likrEventJob','web_id': 'coway'})
    # data_list = AmazonS3('elephants3').getDateHourDataFilter('2022-02-16', 12,{'behavior_type': 'likrEventJob','web_id': 'coway'})


    # with open('data_list.pickle', 'wb') as f:
    #     pickle.dump(data_list, f)


    # with open('data_list.pickle', 'rb') as f:
    #     data_list = pickle.load(f)
    #
    # key_filter = 'event_type'
    # values_filter = 'load'
    # data_list_filter = filterListofDict(data_list, key='event_type', value='load')
    # data_list_filter2 = filterListofDict(data_list_filter, key='web_id', value='nineyi11')
    # data_list_filter3 = filterListofDictByList(data_list_filter, key_list=['web_id'], value_list=['nineyi11'])
    # data_list_filter4 = filterListofDictByDict(data_list, dict_criteria={'event_type': 'load','web_id': 'nineyi11'})
    # data_list_filter5 = filterListofDictByDict(data_list, dict_criteria={'event_type': None,'web_id': None})






    #
    #
    # elephants3 = AmazonS3('elephants3')
    # key_filter = 'event_type'
    # values_filter = 'load'
    # # objects = elephants3.getLatestObjects()
    # objects = elephants3.getLatestMinObjects()
    # n = AmazonS3._CountObejects(objects)
    # print(f"number of objects: {n}")
    # data_dict, i = {}, 0
    # data_list_filter = []
    # for object in objects:
    #     path_object = object.key
    #     print(path_object) ##path of each Object
    #     data_list = json.loads(elephants3.Read(path_object))
    #     print(f"finish loading {path_object}")
    #     data_list_filter += list(filter(lambda x: key_filter in x.keys(), data_list))
    #     data_list_filter = list(filter(lambda x: values_filter in x[key_filter], filter(lambda x: key_filter in x.keys(), data_list)))
    #     data_list_filter = filterListofDict(data_list, key=key_filter, value=values_filter)
        # for data in json.loads(elephants3.Read(path_object)):
        #     # content = json.loads(data)
        #     data_filter = list(filter(lambda x: x))
        #     if key_filter in data.keys():
        #
        #         data_dict[i] = data
        #         i += 1
                # print(data)
    # datetime_latest = datetime_to_str(datetime.datetime.utcnow(), pattern="%Y/%m/%d/%H")
import uuid
from datetime import datetime, timedelta


class S3ApiIntegration:
    def __init__(self, s3_client, s3_bucket, s3_prefix):
        self.__s3_client = s3_client
        self.__s3_bucket = s3_bucket
        self.__s3_prefix = s3_prefix

    def save_file_by_bytes_result(self, bytes_result, s3_partition_options=None):
        date_key = self.generate_s3_key_path(s3_partition_options)
        self.__s3_client.put_object(Body=bytes_result, Bucket=self.__s3_bucket, Key=date_key)

        return date_key

    def save_file_by_result_list(self, result_list, s3_partition_options=None):
        result_list = bytes("\n".join(result_list), 'utf-8')
        date_key = self.generate_s3_key_path(s3_partition_options)

        self.__s3_client.put_object(Body=result_list, Bucket=self.__s3_bucket, Key=date_key)

        return date_key

    def get_result_list_by_object_key(self, object_key):
        s3_key_path = f'{self.__s3_prefix}/{object_key}'
        object_json_list = self.__s3_client.get_object(Bucket=self.__s3_bucket, Key=s3_key_path)

        return object_json_list['Body'].read().decode('utf-8').splitlines()

    def generate_s3_key_path(self, options=None):
        partition_type = options['partition_type'] if options and 'partition_type' in options else None
        partition_value = options['partition_value'] if options and 'partition_value' in options else None

        prefix = self.__s3_prefix
        if partition_type:
            partition_prefix = get_partition(partition_type, partition_value)
            prefix = f"{prefix}/{partition_prefix}"

        filename = self.get_filename(prefix, options)
        return f"{prefix}/{filename}"

    def get_filename(self, prefix, options=None):
        options = options if options else {}
        if 'filename' in options:
            filename = options['filename']
            return filename

        unique_file_by_partition = options['unique_file_by_partition'] if 'unique_file_by_partition' in options else True

        if unique_file_by_partition:
            object_list = get_objects_by_prefix(self.__s3_client, self.__s3_bucket, prefix)
            object_list = remove_prefix_from_object_list(prefix, object_list)

            if object_list:
                filename = object_list[0]
                return filename

        file_suffix = options['file_suffix'] if 'file_suffix' in options else None

        generated_filename = uuid.uuid4().hex
        filename = f'{generated_filename}{file_suffix}' if file_suffix else generated_filename
        return filename


def get_partition(partition_type, partition_value):
    partitions = {
        "yesterday": lambda v: (datetime.today() - timedelta(days=1)).strftime("%Y/%m/%d"),
        "previous_month": lambda v: (datetime.today().replace(day=1) - timedelta(days=1)).strftime("%Y/%m"),
        "previous_week": lambda v: (datetime.today() - timedelta(days=7)).strftime("%Y/%m/%d"),
        "yyyy_mm_dd": lambda v: v.strftime("%Y/%m/%d"),
        "yyyy_mm": lambda v: v.strftime("%Y/%m"),
        "yyyy_mm_dd_key": lambda v: f'{v["date"].strftime("%Y/%m/%d")}/{v["key"]}',
        "yyyy_mm_dd_hh": lambda v: v.strftime("%Y/%m/%d/%H"),
        "yyyy_mm_dd_hh_key": lambda v: f'{v["date"].strftime("%Y/%m/%d/%H")}/{v["key"]}',
        "key_yyyy_mm_dd": lambda v: f'{v["key"]}/{v["date"].strftime("%Y/%m/%d")}',
        "custom": lambda v: v
    }

    return partitions[partition_type](partition_value)


def get_object_from_s3(s3_client, bucket, key_path):
    object_json_list = s3_client.get_object(Bucket=bucket, Key=key_path)

    return object_json_list['Body']


def get_result_list_from_s3(s3_client, bucket, key_path):
    object_json_list = get_object_from_s3(s3_client, bucket, key_path)

    return object_json_list.read().decode('utf-8').splitlines()


def get_objects_by_prefix(s3_client, bucket, prefix):
    if not prefix.endswith('/'):
        prefix = f'{prefix}/'

    result = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    next_token = result['NextContinuationToken'] if 'NextContinuationToken' in result else None
    object_list = result['Contents'] if 'Contents' in result else []

    while next_token:
        result = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=next_token)
        next_token = result['NextContinuationToken'] if 'NextContinuationToken' in result else None
        object_list = object_list + result['Contents']

    return object_list


def remove_prefix_from_object(prefix, obj):
    prefix_length = len(f'{prefix}/')
    keys_without_prefix = obj['Key'][prefix_length:]

    return keys_without_prefix


def remove_prefix_from_object_list(prefix, list_object):
    prefix_length = len(f'{prefix}/')
    keys_without_prefix = [obj['Key'][prefix_length:] for obj in list_object]

    return keys_without_prefix


def extract_partitions_and_object_name_by_object_list(prefix, list_object):
    keys_without_prefix = remove_prefix_from_object_list(prefix, list_object)
    partitions_and_object_name_result = list(map(extract_partitions_and_object_name, keys_without_prefix))

    return partitions_and_object_name_result


def extract_partitions_and_object_name(object_key):
    partitions_and_object_name_splitted = object_key.rsplit('/', 1)

    if len(partitions_and_object_name_splitted) == 1:
        object_name = partitions_and_object_name_splitted[0]

        partitions_and_object_name_result = {
            'partitions': [],
            'object_name': object_name
        }

        return partitions_and_object_name_result

    partitions = partitions_and_object_name_splitted[0]
    object_name = partitions_and_object_name_splitted[1]

    partitions_and_object_name_result = {
        'partitions': partitions.split('/'),
        'object_name': object_name
    }

    return partitions_and_object_name_result

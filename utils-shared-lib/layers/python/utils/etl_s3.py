import io
import uuid
import pandas as pd

from datetime import datetime, timedelta
from utils import s3 as s3_utils
from utils.s3 import S3ApiIntegration

class S3ApiETL(S3ApiIntegration):
    def __init__(self, s3_client, s3_bucket, s3_prefix):
        super(S3ApiETL, self).__init__(s3_client, s3_bucket, s3_prefix)
        self.__s3_client = s3_client

    def load_script(self, bucket, key):
        script = self.__s3_client.get_object(Bucket=bucket, Key=key)
        script = script['Body'].read().decode('utf-8')
        script_output = {}

        exec(script, script_output)
        return script_output

    def get_df_from_s3_by_keypath(self, bucket, key_path, file_type="json"):
        read_object_dict = {
            "json": lambda obj: pd.read_json(io.BytesIO(obj.read()), lines=True)
        }

        obj = s3_utils.get_object_from_s3(self.__s3_client, bucket, key_path)
        s3_df = read_object_dict[file_type](obj)

        return s3_df

    def add_partition_on_df(self, s3_df, prefix, s3_object):
        key_without_prefix = s3_utils.remove_prefix_from_object(prefix, s3_object)
        partition_extracted = s3_utils.extract_partitions_and_object_name(key_without_prefix)

        for index, partition_value in enumerate(partition_extracted['partitions']):
            s3_df[f'partition_{index}'] = partition_value

    def get_df_from_s3(self, bucket, prefix, file_type="json"):
        objects_list = s3_utils.get_objects_by_prefix(self.__s3_client, bucket, prefix)
        s3_df = pd.DataFrame()

        for s3_object in objects_list:
            tmp_df = self.get_df_from_s3_by_keypath(bucket, s3_object['Key'], file_type)
            self.add_partition_on_df(tmp_df, prefix, s3_object)
            s3_df = s3_df.append(tmp_df, ignore_index=True)

        return s3_df

    def save_df(self, df_result, s3_partition_options=None):
        partition_type = s3_partition_options['partition_type'] if s3_partition_options else None

        if partition_type != 'column_name':
            output_json = df_result.to_json(orient='records', lines=True)
            output_json = bytes(output_json, 'utf-8')

            last_key_path = self.save_file_by_bytes_result(output_json, s3_partition_options)

            return last_key_path

        partition_value = s3_partition_options['partition_value'] if s3_partition_options else None

        df_result_list = [pd.DataFrame(y) for x, y in df_result.groupby(partition_value, as_index=False)]

        last_key_path = None
        s3_partition_options['partition_type'] = 'custom'

        for df_result_item in df_result_list:
            output_json = df_result_item.to_json(orient='records', lines=True)
            output_json = bytes(output_json, 'utf-8')

            s3_partition_options['partition_value'] = df_result_item[partition_value].iloc[0]

            last_key_path = self.save_file_by_bytes_result(output_json, s3_partition_options)

        return last_key_path

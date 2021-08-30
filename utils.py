import os
import json
import yaml
import jmespath
from datetime import datetime, timezone
from math import ceil, floor
from collections.abc import Iterable
import utils.utils_log as ul
import boto3
from botocore.exceptions import ClientError

from jmespath_options.datalake_functions import JmesPath_Functions
jmespath_functions = jmespath.Options(custom_functions=JmesPath_Functions())

s = boto3.Session()
s3_client = s.client('s3')
sqs_client= s.client('sqs')
glue_client = s.client('glue')


## Lovely Pure-ish Functions ##

def as_json(obj, indent=4):
    return json.dumps(obj, indent=indent)


def write_str_to_file(filename, string):
    with open(filename, 'w') as f:
        f.write(string)


def json_to_file(file_name, obj, indent=4, operation='w'):
    with open(file_name, operation) as f:
        f.write(json.dumps(obj, indent=indent))


def write_to_json_file(filename, obj):
    with open(filename, 'w') as f:
        json.dump(obj, f)


def read_from_json_file(file_name):
    with open(file_name) as f:
        obj = json.load(f)
    return obj


def james(search, obj, assert_not_none = True, options = jmespath_functions):
    """ james
        jmespath.search with assert that the result is not None
    """
    try:
        result = jmespath.search(search, obj, options=options)
        # print('search=',search)
        # print('result=',result)
        if assert_not_none:
            assert result is not None, f'{search} not found for {str(obj)[:500]}'
        return result
    except AssertionError as error:
        raise AssertionError(f'james_error: {error}')
    except ValueError as error:
        raise ValueError(f'james_error: {error}')


def get_utc_timestamp_str():
    return f'{datetime.utcnow().replace(tzinfo=timezone.utc):%Y-%m-%dT%H:%M:%S.%f%z}'


##  AWS utils ##

def get_utc_date_str():
    return f'{datetime.utcnow().replace(tzinfo=timezone.utc):%Y-%m-%d}'


def get_utc_time_str():
    return f'{datetime.utcnow().replace(tzinfo=timezone.utc):%H:%M:%S.%f%z}'


def get_msg_str(msg_obj, separators=(',', ':')):
    if isinstance(msg_obj, str):
        msg_str = msg_obj
    else:
        try:
            msg_str = json.dumps(msg_obj, separators=separators)
        except TypeError:
            msg_str = str(msg_obj)
    return msg_str


#TODO: maybe change flat_list to use itertools.chain.from_iterable: list(itertools.chain.from_iterable(a))
def flat_list(lst: list) -> list:
    """ flat_list
        given a list of lists of variable depth
        return a flatterned list of elements
    """
    if not isinstance(lst, list):
        raise TypeError('1st parameter lst must be a list')
    result = []
    for i in lst:
        if isinstance(i, list):
            result.extend(flat_list(i))
        else:
            result.extend([i])
    return result


def lower_dict_keys(d: dict) -> dict:
    """ lower_dict_keys
        given a dictionary of  mixed typed items
        return a dictionary where the keys are converted to lower case
    """
    return {
        k.lower(): v for k, v in d.items()
    }

def deep_lower_dict_keys(d: dict) -> dict:
    """ deep_lower_dict_keys
        given a dictionary of mixed typed items
        return a dictionary where the keys are converted to lower case
          and also for all the items that are dicts
    """
    if isinstance(d, str):
        return d
    if isinstance(d, dict):
        return {
            k.lower(): deep_lower_dict_keys(v)
            for k, v in d.items()
        }
    elif isinstance(d, list):
        return [
            deep_lower_dict_keys(v)
            for v in d
        ]
    else:
        return d


def lower_dict(d: dict) -> dict:
    """ lower_dict
        given a dictionary of  mixed typed items
        return a dictionary where the keys and the str values
        are converted to lower case
    """
    return {
        k.lower(): v.lower() if isinstance(v, str) else v
        for k, v in d.items()
    }


def lower_seq(seq : Iterable) -> Iterable:
    """ lower_seq
        given a an iterator seq
        return a generator of seq with all strings as lower case using the tx function
    """
    return (
        value.lower() if isinstance(value, str) else value
        for value in seq
    )


def lower_list(lst: list) -> list:
    """ lower_list
        given a list of strings, return the strings as lower
    """
    if not isinstance(lst, list):
        raise TypeError('1st parameter lst must be a list')
    return list(lower_seq(lst))


def is_any_value_blank(value_list: Iterable) -> bool:
    """ is_any_value_blank
        return True if any of the values in the list is None or empty
    """
    blank_value_found = False
    for value in value_list:
        if value is None or (hasattr(value, '__len__') and len(value)==0):
            blank_value_found = True
    return blank_value_found


def ensure_dict_values_are_str(d):
    if not isinstance(d, dict):
        TypeError( f'needs to be a dictionary')
    for key, val in d.items():
        if not isinstance(val, str):
            raise TypeError( f'value for key {key} is {val} which is {type(val)}; it needs to be a string')


def sort_on_keys(lst : Iterable) -> list:
  """ sort_on_keys
      given an iterator of dictionaries
      return it as a list sorted on the set of keys
  """
  return sorted(lst, key = lambda d: str(d.keys()))


## Logging Functions ##

#TODO: add lambda context corellation key for lambda functions
# _lambda_context_ = None
# def _get_context_request_id():
#     global _lambda_context_
#     context = _lambda_context_
#     if context is not None:
#         retrun dict(
#             aws_request_id = context.aws_request_id,
#             remaining_time = context.get_remaining_time_in_millis(),
#             )
#     else
#         return dict()

# def log_context(context):
#     global _lambda_context_
#     _lambda_context_ = context
#     if context is not None:
#         log(
#             invoked_function_arn = context.invoked_function_arn,
#             log_stream_name = context.log_stream_name,
#             log_group_name = context.log_group_name,
#             aws_request_id = context.aws_request_id,
#             memory_limit_in_mb = context.memory_limit_in_mb,
#             remaining_time = context.get_remaining_time_in_millis(),
#             )




## SQS Functions ##

def create_sqs_queue(queue_name, tags, visibility_timeout_secs=30):
    response = sqs_client.create_queue(
        QueueName=queue_name,
        Attributes=dict(
            VisibilityTimeout=visibility_timeout_secs
        ),
        tags=tags
    )
    return response


def delete_sqs_queue(queue_url):
    response = sqs_client.delete_queue(
        QueueUrl=queue_url
    )
    return response


def get_batch_bytes():
    """ get_batch_bytes
        used for sizing sqs messages anthough might also be used for sns
        256 Kb is the default limit
    """
    return int(os.environ.get('BATCH_BYTES', 256*1024))


def calc_avg_items_per_batch(item_count, batch_size, total_size):
    """ calc_avg_items_per_batch
        return the average number of items that can fit into a batch
    """
    if item_count < 0 or total_size < 0:
        raise( ValueError('item_count and total_size must be at least zero'))
    if batch_size < 1:
        raise( ValueError('only accept batch size >= 1 byte'))
    if item_count == 0 or total_size == 0:
        return 0
    number_of_batches = ceil(total_size/batch_size)
    if item_count < number_of_batches:
        raise( ValueError('items are too big, the items size is bigger than the batch size'))
    items_per_batch = floor(item_count/number_of_batches)
    return items_per_batch


def generate_json_item_batch_per_size_limit(item_list, size_limit):
    """ generate_json_item_batch_per_size_limit
        yield a lists of items that are less than size_limit when rendered as json
        and can thus fit into a batch
    """
    print(f'json size: {size_in_json(item_list)}, size_limit: {size_limit}')
    if size_in_json(item_list) <= size_limit:
        print('size is good')
        yield item_list
    else:
        mid_index = int(len(item_list)/2)
        print('size is too big splitting in two')
        yield from generate_json_item_batch_per_size_limit(item_list[:mid_index], size_limit)
        yield from generate_json_item_batch_per_size_limit(item_list[mid_index+1:], size_limit)
        # ref: https://stackoverflow.com/questions/38254304/can-generators-be-recursive


def generate_json_item_batches(item_list, items_limit, size_limit):
    """ generate_item_batches
        yield json strings (containing a list of items) that don't exceed batch_size in number of size
    """
    for i in range(0, len(item_list), items_limit):
        for batch in generate_json_item_batch_per_size_limit(item_list[i:i+items_limit],size_limit):
            yield batch


def create_sqs_message_header(**kwargs):
    return { **kwargs }


def create_sqs_message_with_header(header, data, datakey='data'):
    return { **header , **{ datakey : data } }  


def generate_batches_with_header( batches, header ):
    assert isinstance(batches, Iterable)
    assert isinstance(header, dict)
    for batch in batches:
         yield create_sqs_message_with_header(header, batch)


def send_msg_obj_to_sqs(queue_url, msg_obj):
    msg_str = get_msg_str(msg_obj)
    response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody= msg_str,
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        return response
    return None


def send_batches_to_sqs( queue_url, batches ):
    """ send_batches_to_sqs
        sends each batch in baches to sqs queue
        clearly requires batches to be split into suitable size for sqs already
    """
    ul.log(status='starting send batches', queue_url=queue_url)
    for batch_id, batch in enumerate(batches):
        ul.log(batch=batch_id)
        response = send_msg_obj_to_sqs(
            queue_url = queue_url,
            msg_obj = batch,
        )
        if response is not None:
            ul.log(
                Error = 'Failed to send batch to queue',
                batch_id = batch_id, 
                httpstatuscode = response["ResponseMetadata"]["HTTPStatusCode"], 
                queue_url = queue_url,
                response = response,
                message = batch
                )


def send_msg_list_to_sqs(queue_url, msg_list):
    """ send_msg_list_to_sqs
        sends a list of messages to sqs queue breaking the 
        list into batches of suitable size
    """
    assert isinstance(msg_list, Iterable)

    items_per_batch = calc_avg_items_per_batch(
                item_count = len(msg_list),
                batch_size = get_batch_bytes(),
                total_size = size_in_json(msg_list))
    ul.log(avg_items_per_batch = items_per_batch , queue_url=queue_url)
    for batch in generate_json_item_batches(
                msg_list, 
                items_per_batch, 
                get_batch_bytes()):
        send_msg_obj_to_sqs(queue_url, batch)


def size_in_json(obj, separators=(',', ':')):
    """ size_in_json
        return the size of item when rendered as json
    """
    return len(json.dumps(obj, separators=separators))


## S3 Functions ##

def make_s3_key(prefix='', key_pattern='test.json', suffix='', **kwargs):
    filled_pattern = key_pattern.format(**kwargs)
    key = prefix + filled_pattern + suffix
    return key


def write_content_to_s3(bucket, key, content):
    response = s3_client.put_object(
        Body = content,
        Bucket = bucket,
        Key = key,
        ServerSideEncryption = 'aws:kms'
    )
    return response


def get_s3_event_bucket_and_key(event):
    """ get_event_bucket_and_path 
        return a tuple with bucket and key from the event
    """
    ul.log(event=event)
    if event['Records'][0]['EventSource'] == 'aws:sns':
        event = json.loads(event['Records'][0]['Sns']['Message'])
        ul.log(event=event)
    result = jmespath.search(
        'Records[*].s3.{ Bucket : bucket.name, Key : object.key }|[0]', 
        event 
        )
    # ul.log(result=result)
    return (result["Bucket"], result['Key'])

### S3 ###

def make_tagset(**d):
    ensure_dict_values_are_str(d)
    return {
        'TagSet': [
            dict(Key = k, Value = v)
            for k, v in d.items()
        ]
    }


def tag_s3_object(bucket, key, **tags):
    """ tag_s3_object
        add a single tag to an s3 object
        return boto response to put_object_tagging
    """
    return s3_client.put_object_tagging(
        Bucket = bucket,
        Key = key,
        Tagging = make_tagset(**tags)
    )


def get_s3_object_tags(bucket, key):
    """ get_s3_object_tag
        return a dictionary of tags
    """
    response = s3_client.get_object_tagging(
        Bucket=bucket,
        Key=key
    )
    result_tags = {
        tag['Key']: tag['Value']
        for tag in response['TagSet']
    }
    return result_tags


def read_bytes_from_s3(bucket, key):
    response = s3_client.get_object(
        Bucket = bucket,
        Key = key,
    )
    return response['Body'].read()


def read_text_from_s3(bucket, key):
    return read_bytes_from_s3(bucket, key).decode('utf-8')


def read_json_from_s3(bucket, key):
    return json.loads(read_text_from_s3(bucket, key))


def list_s3_keys(bucket, key_prefix):
    """
     list_s3_keys
     https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
     """
    response =  s3_client.list_objects_v2(
        Bucket = bucket,
        Prefix = key_prefix
    )
    keys_list = jmespath.search('Contents[].Key',response)
    return keys_list


def s3_object_exists(bucket, key):
    lok = list_s3_keys(bucket, key)
    return lok is not None and len(lok) == 1 and lok == [key]


def delete_s3_object(bucket, key):
    """
    delete_s3_object
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object
    """
    return s3_client.delete_object(
        Bucket = bucket,
        Key = key
    )


def make_stage_file_key(source, model, date, epoch):
    return make_s3_key(key_pattern='{source}/{model}/{date}/stg_{model}_{source}_{epoch}',
        source=source,
        model=model,
        date=date,
        epoch=epoch)


def make_stage_error_file_key(source, model, date, epoch):
    return make_s3_key(
        prefix='error/',
        key_pattern='{source}/{model}/{date}/stg_{model}_{source}_{epoch}',
        source=source,
        model=model,
        date=date,
        epoch=epoch)


def make_table_s3_key(table_name):
    """ make_table_s3_key
        return vault table s3 key (e.g. vault/hub_device/hub_device.snappy.parquet)
    """
    # s3://mtdata-datalake-vault-dev/vault/hub_customer/hub_customer.snappy.parquet
    return make_s3_key(
        key_pattern = 'vault/{table_name}/{table_name}.snappy.parquet',
        table_name = table_name
    )


def make_error_table_s3_key(table_name):
    """ make_table_s3_key
        return vault error table s3 key (e.g. error/vault/hub_device/error_hub_device.snappy.parquet)
    """
    # s3://mtdata-datalake-vault-dev/vault/hub_customer/hub_customer.snappy.parquet
    return make_s3_key(
        key_pattern = 'error/vault/{table_name}/error_{table_name}.snappy.parquet',
        table_name = table_name
    )


def deconstruct_stage_file_key(key):
    """ deconstruct_stage_file_key
        return a tuple with source, entity, date and epoch
    """
    (source, model, date, filename) = key.split('/')
    epoch = filename.split('_')[-1]
    return (source, model, date, epoch)


def deconstruct_stage_error_file_key(key):
    """ deconstruct_stage_file_key
        return a tuple with source, entity, date and epoch
    """
    (_, source, model, date, filename) = key.split('/')
    epoch = filename.split('_')[-1]
    return (source, model, date, epoch)


def get_glue_database_definition(database_name):
    try:
        return glue_client.get_database(
            Name=database_name,
        )
    except ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return None
        else:
            raise error


def create_glue_database(database_definition):
    try:
        glue_client.create_database(
            DatabaseInput=database_definition
        )
        ul.log(info=f'Glue database {database_definition["Name"]} has been successfully created')
    except Exception as exc:
        ul.log(error=exc)


def get_error_vault_database_name(vault_database_name):    
    return '_'.join(vault_database_name.split('_')[:-1] + ['error'] + vault_database_name.split('_')[-1:])


def create_error_vault_database_from_vault_database(vault_database_name):

    error_vault_database_name = get_error_vault_database_name(vault_database_name)
    
    if get_glue_database_definition(error_vault_database_name) is None:
        try:
            response = get_glue_database_definition(vault_database_name)
            
            response['Database']['Name']=error_vault_database_name
            response['Database']['Description']='Data Lake Errors'
            keys_to_remove=['CreateTime', 'CatalogId']
            for key in keys_to_remove:
                del response['Database'][key]
            # print('vault_database_definition: ', response['Database'])
            
            create_glue_database(response['Database'])
        except Exception as exc:
            ul.log(error=exc)
    else:
        ul.log(error=f'{error_vault_database_name} already exists')


def get_glue_table_definition(database_name, table_name):
    try:
        return glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
    except ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return None
        else:
            raise error


def create_glue_table(database_name, table_definition):
    try:
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput=table_definition
        )
        ul.log(info=f'Glue table {table_definition["Name"]} has been successfully created')
    except Exception as exc:
        ul.log(error=exc)


def delete_glue_table(database_name, table_name):
    try:
        glue_client.delete_table(
            DatabaseName=database_name,
            Name=table_name
        )
        ul.log(info=f'Glue table {table_name} has been successfully deleted')
    except Exception as exc:
        ul.log(error=exc)


def delete_glue_database(database_name):
    try:
        glue_client.delete_database(
            Name=database_name
        )
        ul.log(info=f'Glue database {database_name} has been successfully deleted')
    except Exception as exc:
        ul.log(error=exc)


def get_error_table_s3_uri(vault_bucket, vault_table_name):
    return f's3://{vault_bucket}/error/vault/{vault_table_name}/'


def get_error_table_name(vault_table_name):
    return f'error_{vault_table_name}'


def get_database_name_from_table_name(vault_table_name):
    return f'error_{vault_table_name}'

def get_error_vault_database_table_from_vault_database_table(vault_database_name, vault_table_name):
    error_vault_database_name = get_error_vault_database_name(vault_database_name)
    error_vault_table_name = get_error_table_name(vault_table_name)
    return get_glue_table_definition(error_vault_database_name, error_vault_table_name)

# TODO: make this glue_table_created(database_name, table_name)
def error_table_created(vault_database_name, vault_table_name):
    error_vault_database_name = get_error_vault_database_name(vault_database_name)
    error_vault_table_name = get_error_table_name(vault_table_name)
    return get_glue_table_definition(error_vault_database_name, error_vault_table_name) is not None
    

# TODO: switch this to two functions:
# error_table_metadata = make_error_table_metadata_for_glue_table(database_name, table_name)
# create_glue_table(bucket, database_name, table_name, table_definition)
def create_error_vault_database_table_from_vault_database_table(vault_bucket, vault_database_name, vault_table_name):
    
    error_vault_database_name = get_error_vault_database_name(vault_database_name)
    error_vault_table_name = get_error_table_name(vault_table_name)

    try:
        if get_glue_database_definition(error_vault_database_name) is None:
            create_error_vault_database_from_vault_database(vault_database_name)

        response = get_glue_table_definition(vault_database_name, vault_table_name)

        response['Table']['Name']=error_vault_table_name
        response['Table']['StorageDescriptor']['Location']=get_error_table_s3_uri(vault_bucket, vault_table_name)
        columns = [
            {
                'Name': 'error_load_ts',
                'Type': 'timestamp',
                'Comment': 'loading timestamp of error rows'
            },
            {
                'Name': 'error_column',
                'Type': 'string',
                'Comment': 'column name which has error value '
            },
            {
                'Name': 'error_value',
                'Type': 'string',
                'Comment': 'error value '
            },
            {
                'Name': 'error_description',
                'Type': 'string',
                'Comment': 'error description for rejected rows'
            },
            {
                'Name': 'stage_path',
                'Type': 'string',
                'Comment': 'data source'
            }
        ]
        for column in columns:
            response['Table']['StorageDescriptor']['Columns'].append(column)
        keys_to_remove=['DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy', 'IsRegisteredWithLakeFormation', 'CatalogId']
        for key in keys_to_remove:
            del response['Table'][key]
        # print('new_table_definition: ', response['Table'])

        create_glue_table(error_vault_database_name, response['Table'])
    except Exception as exc:
        ul.log(error=exc)


def delete_error_vault_database_from_vault_database(vault_database_name):
    error_vault_database_name = get_error_vault_database_name(vault_database_name)
    delete_glue_database(error_vault_database_name)


def delete_error_vault_database_table_from_vault_database_table(vault_database_name, vault_table_name):
    error_vault_database_name = get_error_vault_database_name(vault_database_name)
    error_vault_table_name = get_error_table_name(vault_table_name)
    delete_glue_table(error_vault_database_name, error_vault_table_name)
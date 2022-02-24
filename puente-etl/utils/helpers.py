import re
import string

import shortuuid

alphabet = string.ascii_lowercase + string.digits
su = shortuuid.ShortUUID(alphabet=alphabet)


def shortuuid_random():
    return su.random(length=8)


def to_snake_case(words):

    regex_pattern = r'(?<!^)(?=[A-Z])'

    if isinstance(words, str):
        return re.sub(regex_pattern, '_', words.strip('_')).lower()

    elif isinstance(words, list):
        return [
            re.sub(regex_pattern, '_', word).lower()
            for word in words
        ]
    else:
        print('Helper to_snake_case function received bad input. Expected String or List.')


def get_fields_from_list_of_dicts(data: list):
    top_level_columns = []
    for row in data:
        top_level_columns.extend(list(row.keys()))

    return get_unique_fields_from_list(top_level_columns)


def get_unique_fields_from_list(fields: list):
    return sorted(list(set(fields)))


def get_column_order_by_least_null(df) -> list:
    null_count = df.isnull().sum(axis=0).sort_values()
    return null_count.index.tolist()







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

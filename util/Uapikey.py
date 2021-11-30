import json


def get_token(target:str) -> str:
    """
    Keys are saved in separate json file
        for extra security.
    :param target: type in the name for certain api
    :return: api key in str
    """
    loc = "./security/api.json"  # refract this file to add subtract info.
    with open(loc, 'r') as file:
        dat = json.load(file)
    file.close()

    return dat['apikey'][target]


if __name__ == '__main__':
    # Test
    print(get_token('dart'))
    print(len(get_token('dart')))

    # Test
    print(get_token('datagov'))
    print(len(get_token('datagov')))
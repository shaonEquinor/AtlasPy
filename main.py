from atlas import functions


if __name__ == '__main__':
    primary_keys = ['date', 'id', 'station']

    print(filter(lambda x: x != 'id', primary_keys))

    match_cond = " and ".join([f"data.{keystring} = newData.{keystring}" for keystring in primary_keys])

    print(match_cond)




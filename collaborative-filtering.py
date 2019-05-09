import os, sys
from .preprocessing import preprocess_file


def normalize_ratings(row, item_key):
    key = row[0]
    items = row[1]
    count = 0
    sum = 0
    for item in items:
        if item_key is None or item[0] != item_key:
            count = count + 1
            sum = sum + item[1]
    
    mean = sum / count
    new_items = []
    for item in items:
        if item_key is None or item[0] != item_key:
            new_items.append((item[0], item[1] - mean))

    return (key, new_items)
    

def cos_sim(A,B):
	return( sum(A*B)/sqrt(sum(A^2)*sum(B^2)) )   

if __name__ == '__main__':
    input_file = sys.args[1]
    df = preprocess_file(input_file)
    user_id = 0
    item_id = 540429


    # User - User CF
    # 1. convert to rdd (user_id, (item_id, rating))
    rdd = df.rdd

    item_rdd = rdd.map(lambda tup: (tup[1], (tup[0], tup[2])))
    item_groups = item_rdd.groupByKey()
    predict_item_list = item_groups.filter(lambda k_v: k_v[0] == item_id).first()[1]

    user_rdd = rdd.map(lambda tup: (tup[0], (tup[1], tup[2])))

    # Get items that user 1 has rated
    # 2. groupBy Key
    user_groups = user_rdd.groupByKey()

    user_normalized_groups = user_groups.map(normalize_ratings)
    # 3. for item_id and user_id, get prediction

    # get only users

    
    # 4. move to other file

